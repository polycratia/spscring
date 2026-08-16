CXX ?= c++
CXXFLAGS ?= -std=c++17 -Wall -Wextra -Werror -Iinclude -Itests -pthread
SAN ?= -fsanitize=address,undefined -fno-omit-frame-pointer -g

.PHONY: test race demo clean

test:
	@mkdir -p build
	$(CXX) $(CXXFLAGS) $(SAN) tests/test_spscring.cpp -o build/tests
	./build/tests

# ThreadSanitizer is the only one that can see a missing acquire or release.
# A lock-free queue that has not been through it has not been checked.
race:
	@mkdir -p build
	$(CXX) $(CXXFLAGS) -fsanitize=thread -O1 -g tests/test_spscring.cpp -o build/tests-tsan
	./build/tests-tsan

demo:
	@mkdir -p build
	$(CXX) $(CXXFLAGS) -O2 example/main.cpp -o build/demo
	./build/demo

clean:
	rm -rf build
