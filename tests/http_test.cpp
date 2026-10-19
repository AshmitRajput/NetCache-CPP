// Socket-free tests for the HTTP subset (src/http.cpp).

#include "http.hpp"
#include "test_harness.hpp"

#include <utility>

using namespace netcache::http;

namespace {

std::string req(const std::string& line = "GET /x HTTP/1.1", const std::string& headers = "Host: example.test\r\n") {
    return line + "\r\n" + headers + "\r\n";
}

std::string resp(const std::string& status, const std::string& headers) { return status + "\r\n" + headers + "\r\n"; }

}  // namespace

// ----------------------------------------------------------- request: accept

TEST(request_minimal_valid) {
    std::string raw = req("GET /catalog/items?page=2 HTTP/1.1");
    auto r = parseRequest(raw);
    CHECK(r.state == ParseState::Complete);
    CHECK_EQ(r.request.target, std::string("/catalog/items?page=2"));
    CHECK_EQ(r.consumed, raw.size());
}

TEST(request_header_values_are_trimmed) {
    auto r = parseRequest(req("GET / HTTP/1.1", "Host:   example.test \t\r\nAccept:\ttext/html  \r\n"));
    CHECK(r.state == ParseState::Complete);
    CHECK_EQ(r.request.headers.size(), size_t(2));
    CHECK_EQ(r.request.headers[0].value, std::string("example.test"));
    CHECK_EQ(r.request.headers[1].value, std::string("text/html"));
}

TEST(request_consumed_excludes_pipelined_bytes) {
    std::string head = req();
    auto r = parseRequest(head + "GET /second HTTP/1.1\r\n");
    CHECK(r.state == ParseState::Complete);
    CHECK_EQ(r.consumed, head.size());
}

TEST(request_exactly_64_headers_ok_65_rejected) {
    std::string hs = "Host: a\r\n";
    for (int i = 0; i < 63; ++i) hs += "X-" + std::to_string(i) + ": v\r\n";
    CHECK(parseRequest(req("GET / HTTP/1.1", hs)).state == ParseState::Complete);
    hs += "X-extra: v\r\n";
    auto r = parseRequest(req("GET / HTTP/1.1", hs));
    CHECK(r.state == ParseState::Error);
    CHECK_EQ(r.errorStatus, 431);
}

TEST(request_byte_at_a_time_never_completes_early) {
    std::string raw = req("GET /byte HTTP/1.1", "Host: example.test\r\nAccept: */*\r\n");
    for (size_t n = 0; n < raw.size(); ++n) {
        auto r = parseRequest(std::string_view(raw).substr(0, n));
        CHECK(r.state == ParseState::NeedMore);
    }
    auto done = parseRequest(raw);
    CHECK(done.state == ParseState::Complete);
    CHECK_EQ(done.request.target, std::string("/byte"));
}

// ----------------------------------------------------------- request: reject

TEST(request_rejection_table) {
    std::string nulValue = "Host: a";
    nulValue.push_back('\0');
    nulValue += "b\r\n";

    struct Case {
        const char* name;
        std::string raw;
        int status;
    };
    const std::vector<Case> cases = {
        {"POST is a known method", req("POST / HTTP/1.1"), 405},
        {"unknown method", req("BREW / HTTP/1.1"), 501},
        {"invalid method token", req("G@T / HTTP/1.1"), 400},
        {"HTTP/1.0", req("GET / HTTP/1.0"), 505},
        {"HTTP/2.0", req("GET / HTTP/2.0"), 505},
        {"garbage version", req("GET / FOO"), 400},
        {"absolute-form target", req("GET http://evil.test/ HTTP/1.1"), 400},
        {"asterisk target", req("GET * HTTP/1.1"), 400},
        {"leading double slash", req("GET //evil.test/x HTTP/1.1"), 400},
        {"dot segment ..", req("GET /a/../b HTTP/1.1"), 400},
        {"dot segment .", req("GET /a/./b HTTP/1.1"), 400},
        {"trailing dot segment", req("GET /a/.. HTTP/1.1"), 400},
        {"encoded dot", req("GET /a/%2e%2E/b HTTP/1.1"), 400},
        {"encoded NUL", req("GET /a%00b HTTP/1.1"), 400},
        {"bad percent escape", req("GET /a%zz HTTP/1.1"), 400},
        {"truncated percent escape", req("GET /a% HTTP/1.1"), 400},
        {"space in target (4 parts)", req("GET /a b HTTP/1.1"), 400},
        {"fragment", req("GET /a#frag HTTP/1.1"), 400},
        {"backslash", req("GET /a\\b HTTP/1.1"), 400},
        {"non-ASCII byte", req("GET /caf\xc3\xa9 HTTP/1.1"), 400},
        {"target too long", req("GET /" + std::string(2048, 'a') + " HTTP/1.1"), 414},
        {"empty request line", "\r\n\r\n", 400},
        {"double space in request line", req("GET  / HTTP/1.1"), 400},
        {"bare LF in headers", req("GET / HTTP/1.1", "Host: x\nFoo: y\r\n"), 400},
        {"bare CR in headers", req("GET / HTTP/1.1", "Host: x\rFoo: y\r\n"), 400},
        {"obs-fold", req("GET / HTTP/1.1", "Host: x\r\n continued\r\n"), 400},
        {"space before colon", req("GET / HTTP/1.1", "Host : x\r\n"), 400},
        {"empty header name", req("GET / HTTP/1.1", "Host: x\r\n: v\r\n"), 400},
        {"header without colon", req("GET / HTTP/1.1", "Host: x\r\nNoColon\r\n"), 400},
        {"NUL in header value", req("GET / HTTP/1.1", nulValue), 400},
        {"missing Host", req("GET / HTTP/1.1", "Accept: */*\r\n"), 400},
        {"duplicate Host", req("GET / HTTP/1.1", "Host: a\r\nHost: a\r\n"), 400},
        {"Host with userinfo", req("GET / HTTP/1.1", "Host: a@b\r\n"), 400},
        {"empty Host", req("GET / HTTP/1.1", "Host:\r\n"), 400},
        {"Content-Length + Transfer-Encoding", req("GET / HTTP/1.1", "Host: a\r\nContent-Length: 0\r\nTransfer-Encoding: chunked\r\n"), 400},
        {"Transfer-Encoding alone", req("GET / HTTP/1.1", "Host: a\r\nTransfer-Encoding: chunked\r\n"), 400},
        {"duplicate Content-Length 0", req("GET / HTTP/1.1", "Host: a\r\nContent-Length: 0\r\nContent-Length: 0\r\n"), 400},
        {"body announced", req("GET / HTTP/1.1", "Host: a\r\nContent-Length: 5\r\n"), 400},
        {"Expect", req("GET / HTTP/1.1", "Host: a\r\nExpect: 100-continue\r\n"), 417},
        {"Upgrade", req("GET / HTTP/1.1", "Host: a\r\nUpgrade: websocket\r\n"), 501},
        {"oversized head with terminator",
         req("GET / HTTP/1.1", "Host: a\r\nX-Pad: " + std::string(9000, 'a') + "\r\n"), 431},
    };
    for (const auto& c : cases) {
        auto r = parseRequest(c.raw);
        if (r.state != ParseState::Error || r.errorStatus != c.status) {
            throw th::Failure{std::string("case \"") + c.name + "\": expected error " + std::to_string(c.status) +
                              ", got state=" + std::to_string(static_cast<int>(r.state)) +
                              " status=" + std::to_string(r.errorStatus)};
        }
    }
}

TEST(request_early_rejection_before_terminator_arrives) {
    auto lf = parseRequest("GET / HTTP/1.1\nHost: x");
    CHECK(lf.state == ParseState::Error);
    CHECK_EQ(lf.errorStatus, 400);

    auto big = parseRequest("GET / HTTP/1.1\r\nX: " + std::string(9000, 'a'));
    CHECK(big.state == ParseState::Error);
    CHECK_EQ(big.errorStatus, 431);

    CHECK(parseRequest("GET / HTTP/1.1\r\nHost: x\r").state == ParseState::NeedMore);  // CR may be completed
}

// -------------------------------------------------------------- upstream req

TEST(upstream_request_is_rebuilt_not_copied) {
    auto r = parseRequest(req("GET /a?b=1 HTTP/1.1",
                              "Host: client.test\r\nAccept: text/plain\r\nCookie: s=1\r\nAccept-Encoding: gzip\r\n"
                              "X-Forwarded-For: 1.2.3.4\r\n"));
    CHECK(r.state == ParseState::Complete);
    CHECK_EQ(buildUpstreamRequest(r.request, "127.0.0.1:9000"),
             std::string("GET /a?b=1 HTTP/1.1\r\nHost: 127.0.0.1:9000\r\nConnection: close\r\nAccept: text/plain\r\n\r\n"));
}

TEST(upstream_request_drops_duplicate_accept) {
    auto r = parseRequest(req("GET / HTTP/1.1", "Host: a\r\nAccept: a/b\r\nAccept: c/d\r\n"));
    CHECK(r.state == ParseState::Complete);
    CHECK_EQ(buildUpstreamRequest(r.request, "u:1"), std::string("GET / HTTP/1.1\r\nHost: u:1\r\nConnection: close\r\n\r\n"));
}

// ------------------------------------------------------------------ response

TEST(response_valid_content_length) {
    std::string raw = resp("HTTP/1.1 200 OK", "Content-Type: text/plain\r\nContent-Length: 5\r\n");
    auto r = parseResponseHead(raw + "hello");
    CHECK(r.state == ParseState::Complete);
    CHECK_EQ(r.head.status, 200);
    CHECK_EQ(r.head.contentLength, size_t(5));
    CHECK_EQ(r.consumed, raw.size());
    CHECK(r.head.framing == Framing::ContentLength);
}

TEST(response_204_and_304_have_no_body) {
    for (const char* line : {"HTTP/1.1 204 No Content", "HTTP/1.1 304 Not Modified"}) {
        auto r = parseResponseHead(resp(line, "Date: x\r\n"));
        CHECK(r.state == ParseState::Complete);
        CHECK(r.head.framing == Framing::NoBody);
        CHECK_EQ(r.head.contentLength, size_t(0));
    }
}

TEST(response_empty_reason_phrase_ok) {
    auto r = parseResponseHead(resp("HTTP/1.1 200", "Content-Length: 0\r\n"));
    CHECK(r.state == ParseState::Complete);
}

TEST(response_byte_at_a_time_never_completes_early) {
    std::string raw = resp("HTTP/1.1 200 OK", "Content-Length: 3\r\n");
    for (size_t n = 0; n < raw.size(); ++n) CHECK(parseResponseHead(std::string_view(raw).substr(0, n)).state == ParseState::NeedMore);
    CHECK(parseResponseHead(raw).state == ParseState::Complete);
}

TEST(response_rejection_table) {
    ResponseLimits small;
    small.maxBodyBytes = 1000;
    struct Case {
        const char* name;
        std::string raw;
    };
    const std::vector<Case> cases = {
        {"chunked", resp("HTTP/1.1 200 OK", "Transfer-Encoding: chunked\r\n")},
        {"chunked + content-length", resp("HTTP/1.1 200 OK", "Content-Length: 5\r\nTransfer-Encoding: chunked\r\n")},
        {"duplicate Content-Length, same value", resp("HTTP/1.1 200 OK", "Content-Length: 5\r\nContent-Length: 5\r\n")},
        {"duplicate Content-Length, different", resp("HTTP/1.1 200 OK", "Content-Length: 5\r\nContent-Length: 6\r\n")},
        {"non-numeric Content-Length", resp("HTTP/1.1 200 OK", "Content-Length: abc\r\n")},
        {"signed Content-Length", resp("HTTP/1.1 200 OK", "Content-Length: -1\r\n")},
        {"Content-Length with plus", resp("HTTP/1.1 200 OK", "Content-Length: +5\r\n")},
        {"Content-Length list", resp("HTTP/1.1 200 OK", "Content-Length: 5, 5\r\n")},
        {"overflowing Content-Length", resp("HTTP/1.1 200 OK", "Content-Length: 99999999999999999999999\r\n")},
        {"Content-Length above limit", resp("HTTP/1.1 200 OK", "Content-Length: 1001\r\n")},
        {"no framing at all", resp("HTTP/1.1 200 OK", "Content-Type: text/plain\r\n")},
        {"1xx", resp("HTTP/1.1 100 Continue", "")},
        {"HTTP/2 status line", resp("HTTP/2 200", "Content-Length: 0\r\n")},
        {"garbage status line", resp("NOT HTTP AT ALL", "")},
        {"two-digit status", resp("HTTP/1.1 20 OK", "Content-Length: 0\r\n")},
        {"bare LF in response head", "HTTP/1.1 200 OK\nContent-Length: 0\r\n\r\n"},
        {"obs-fold in response", resp("HTTP/1.1 200 OK", "X: a\r\n b\r\nContent-Length: 0\r\n")},
    };
    for (const auto& c : cases) {
        auto r = parseResponseHead(c.raw, small);
        if (r.state != ParseState::Error) throw th::Failure{std::string("case \"") + c.name + "\" was not rejected"};
    }
}

TEST(response_max_body_boundary_accepted) {
    ResponseLimits lim;
    lim.maxBodyBytes = 1000;
    CHECK(parseResponseHead(resp("HTTP/1.1 200 OK", "Content-Length: 1000\r\n"), lim).state == ParseState::Complete);
}

// ------------------------------------------------------------- client output

TEST(client_response_strips_hop_by_hop_but_keeps_framing) {
    auto r = parseResponseHead(resp("HTTP/1.1 200 OK",
                                    "Connection: keep-alive, X-Private\r\nKeep-Alive: timeout=5\r\nX-Private: secret\r\n"
                                    "X-Public: ok\r\nProxy-Authenticate: Basic\r\nContent-Length: 2\r\n"));
    CHECK(r.state == ParseState::Complete);
    std::string out = buildClientResponse(r.head, "hi");
    CHECK(out.find("X-Public: ok\r\n") != std::string::npos);
    CHECK(out.find("Content-Length: 2\r\n") != std::string::npos);
    CHECK(out.find("X-Private") == std::string::npos);
    CHECK(out.find("Keep-Alive") == std::string::npos);
    CHECK(out.find("Proxy-Authenticate") == std::string::npos);
    CHECK(out.find("keep-alive") == std::string::npos);
    CHECK(out.find("Connection: close\r\n") != std::string::npos);
    CHECK_EQ(out.substr(out.size() - 2), std::string("hi"));
}

TEST(client_response_never_strips_content_length_even_if_listed_in_connection) {
    auto r = parseResponseHead(resp("HTTP/1.1 200 OK", "Connection: Content-Length\r\nContent-Length: 2\r\n"));
    CHECK(r.state == ParseState::Complete);
    CHECK(buildClientResponse(r.head, "hi").find("Content-Length: 2\r\n") != std::string::npos);
}

TEST(error_response_is_well_formed) {
    std::string out = buildErrorResponse(405);
    CHECK(out.rfind("HTTP/1.1 405 Method Not Allowed\r\n", 0) == 0);
    CHECK(out.find("Allow: GET\r\n") != std::string::npos);
    auto body = out.substr(out.find("\r\n\r\n") + 4);
    CHECK(out.find("Content-Length: " + std::to_string(body.size()) + "\r\n") != std::string::npos);
}

int main() { return th::runAll(); }
