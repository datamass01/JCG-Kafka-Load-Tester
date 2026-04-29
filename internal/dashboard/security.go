package dashboard

import (
	"bufio"
	"crypto/subtle"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// requestLogger replaces chi's middleware.Logger with one that scrubs the
// "token" query parameter before writing the access-log line.
func requestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := &statusRecorder{ResponseWriter: w, status: 200}
		next.ServeHTTP(ww, r)
		log.Printf("%s %s %d %s", r.Method, redactToken(r), ww.status, time.Since(start))
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// Hijack delegates to the wrapped writer so WebSocket upgrades keep working
// when this middleware is in the chain.
func (s *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := s.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, errors.New("hijack not supported")
}

func (s *statusRecorder) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// kafkaNameRe matches the legal Kafka topic / consumer-group character set.
// Kafka itself enforces [a-zA-Z0-9._-] with length 1..249, so anything outside
// that range cannot survive a real broker round-trip and is almost certainly a
// payload attempt.
var kafkaNameRe = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,249}$`)

func validKafkaName(s string) bool {
	return kafkaNameRe.MatchString(s)
}

// validBrokerList accepts a comma-separated host:port list. Each entry must
// be a hostname or IPv4/IPv6 literal followed by :port. This blocks payloads
// like "<img src=x ...>" from being accepted as a broker address.
var brokerEntryRe = regexp.MustCompile(`^[a-zA-Z0-9._\-\[\]:]+:[0-9]{1,5}$`)

func validBrokerEntry(s string) bool {
	return brokerEntryRe.MatchString(s)
}

// brokerHostAllowed enforces the SSRF guard for ad-hoc broker connections.
// Hosts already declared in cfg.KafkaInstances or cfg.Security.AllowedBrokerHosts
// are always permitted. Otherwise the host must resolve to a public IP unless
// AllowPrivateBrokers is set (e.g., for docker-compose dev).
func (s *Server) brokerHostAllowed(hostPort string) error {
	host, _, err := net.SplitHostPort(hostPort)
	if err != nil {
		return fmt.Errorf("invalid host:port: %w", err)
	}
	host = strings.ToLower(host)

	// 1. YAML-declared instances are always allowed.
	for _, inst := range s.cfg.KafkaInstances {
		if inst.AdHoc {
			continue
		}
		for _, b := range inst.Brokers {
			h, _, err := net.SplitHostPort(b)
			if err == nil && strings.EqualFold(h, host) {
				return nil
			}
		}
	}
	// 2. Explicit operator allow-list.
	for _, h := range s.cfg.Security.AllowedBrokerHosts {
		if strings.EqualFold(strings.TrimSpace(h), host) {
			return nil
		}
	}
	// 3. Otherwise require a non-private, non-loopback, non-link-local IP.
	if s.cfg.Security.AllowPrivateBrokers {
		return nil
	}
	ips, err := net.LookupIP(host)
	if err != nil || len(ips) == 0 {
		return fmt.Errorf("host did not resolve")
	}
	for _, ip := range ips {
		if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() ||
			ip.IsLinkLocalMulticast() || ip.IsUnspecified() ||
			ip.IsInterfaceLocalMulticast() || ip.IsMulticast() {
			return fmt.Errorf("private or loopback broker rejected")
		}
	}
	return nil
}

// authMiddleware enforces a shared bearer token when one is configured.
// If no token is configured the middleware is a no-op so local development
// keeps working without setup.
func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := s.cfg.Security.AuthToken
		if token == "" {
			next.ServeHTTP(w, r)
			return
		}
		// Accept token via Authorization: Bearer <token> or ?token=<token>
		// (the query-string form lets the WebSocket carry it without custom headers).
		got := ""
		if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
			got = strings.TrimPrefix(h, "Bearer ")
		} else if q := r.URL.Query().Get("token"); q != "" {
			got = q
		}
		if subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// corsMiddleware mirrors the request Origin only when it appears in the
// configured allow-list. When no allow-list is configured the middleware is
// inert (no CORS headers at all), which keeps the same-origin SPA fully
// functional while disallowing cross-origin browser requests by default.
func (s *Server) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" && s.originAllowed(origin) {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
			w.Header().Set("Access-Control-Allow-Credentials", "true")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) originAllowed(origin string) bool {
	for _, o := range s.cfg.Security.AllowedOrigins {
		if o != "" && o == origin {
			return true
		}
	}
	return false
}

// wsOriginAllowed validates the Origin header on WebSocket upgrades. The same
// host as the listening dashboard is always allowed so the bundled SPA works
// out of the box; additional origins must be explicitly whitelisted.
func (s *Server) wsOriginAllowed(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		// Browsers always send Origin for WS upgrades; non-browser clients
		// (curl, internal probes) may omit it. Allow only if no allow-list
		// constraint is configured — in that case auth is the gate.
		return s.cfg.Security.AuthToken != "" || len(s.cfg.Security.AllowedOrigins) == 0
	}
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	if strings.EqualFold(u.Host, r.Host) {
		return true
	}
	return s.originAllowed(origin)
}

// csrfGuard rejects state-changing requests whose Origin (or Referer) doesn't
// match the host the dashboard is being served on, or an explicitly allowed
// origin. This protects body-less POSTs that bypass CORS preflight (form-style
// CSRF). Safe methods (GET/HEAD/OPTIONS) are passed through untouched.
func (s *Server) csrfGuard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet, http.MethodHead, http.MethodOptions:
			next.ServeHTTP(w, r)
			return
		}
		ref := r.Header.Get("Origin")
		if ref == "" {
			ref = r.Header.Get("Referer")
		}
		if ref == "" {
			http.Error(w, "missing Origin/Referer", http.StatusForbidden)
			return
		}
		u, err := url.Parse(ref)
		if err != nil || u.Host == "" {
			http.Error(w, "invalid Origin/Referer", http.StatusForbidden)
			return
		}
		if strings.EqualFold(u.Host, r.Host) {
			next.ServeHTTP(w, r)
			return
		}
		// Re-use the configured CORS allow-list — same trust boundary.
		schemeHost := u.Scheme + "://" + u.Host
		if s.originAllowed(schemeHost) {
			next.ServeHTTP(w, r)
			return
		}
		http.Error(w, "cross-origin request rejected", http.StatusForbidden)
	})
}

// redactToken returns the request URL with the "token" query parameter
// replaced by "REDACTED", without mutating r.URL.
func redactToken(r *http.Request) string {
	if r.URL.RawQuery == "" || !strings.Contains(r.URL.RawQuery, "token=") {
		return r.URL.String()
	}
	q := r.URL.Query()
	q.Set("token", "REDACTED")
	cp := *r.URL
	cp.RawQuery = q.Encode()
	return cp.String()
}

// securityHeaders sets a conservative CSP and clickjacking protections on
// every response.
func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set(
			"Content-Security-Policy",
			"default-src 'self'; "+
				"script-src 'self' https://cdn.jsdelivr.net; "+
				"style-src 'self' 'unsafe-inline'; "+
				"connect-src 'self' ws: wss:; "+
				"img-src 'self' data:; "+
				"frame-ancestors 'none'; "+
				"base-uri 'none'",
		)
		next.ServeHTTP(w, r)
	})
}
