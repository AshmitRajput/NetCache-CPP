#pragma once
// Minimal dependency-free test harness. TEST(name) registers a case; CHECK / CHECK_EQ throw on failure.

#include <cstdio>
#include <exception>
#include <functional>
#include <sstream>
#include <string>
#include <vector>

namespace th {

struct Failure {
    std::string message;
};

struct Case {
    const char* name;
    std::function<void()> fn;
};

inline std::vector<Case>& registry() {
    static std::vector<Case> cases;
    return cases;
}

struct Registrar {
    Registrar(const char* name, std::function<void()> fn) { registry().push_back({name, std::move(fn)}); }
};

template <class A, class B>
void checkEq(const A& a, const B& b, const char* ea, const char* eb, const char* file, int line) {
    if (!(a == b)) {
        std::ostringstream o;
        o << file << ":" << line << ": CHECK_EQ(" << ea << ", " << eb << ") failed: [" << a << "] vs [" << b << "]";
        throw Failure{o.str()};
    }
}

inline int runAll() {
    int failed = 0;
    for (const auto& c : registry()) {
        try {
            c.fn();
            std::printf("[ OK ] %s\n", c.name);
        } catch (const Failure& f) {
            ++failed;
            std::printf("[FAIL] %s\n       %s\n", c.name, f.message.c_str());
        } catch (const std::exception& e) {
            ++failed;
            std::printf("[FAIL] %s\n       unexpected exception: %s\n", c.name, e.what());
        }
    }
    std::printf("\n%zu tests, %d failed\n", registry().size(), failed);
    return failed == 0 ? 0 : 1;
}

}  // namespace th

#define TEST(name)                                          \
    static void name();                                     \
    static th::Registrar registrar_##name(#name, name);     \
    static void name()

#define CHECK(cond)                                                                                  \
    do {                                                                                             \
        if (!(cond)) throw th::Failure{std::string(__FILE__) + ":" + std::to_string(__LINE__) +      \
                                       ": CHECK failed: " #cond};                                    \
    } while (0)

#define CHECK_EQ(a, b) th::checkEq((a), (b), #a, #b, __FILE__, __LINE__)
