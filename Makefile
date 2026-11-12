CXX ?= c++
CXXFLAGS ?= -std=c++17 -Wall -Wextra -Werror -Iinclude -Itests -pthread
SAN ?= -fsanitize=address,undefined -fno-omit-frame-pointer -g

.PHONY: test race race-control demo clean

test:
	@mkdir -p build
	$(CXX) $(CXXFLAGS) $(SAN) tests/test_spscring.cpp -o build/tests
	$(CXX) $(CXXFLAGS) $(SAN) tests/test_heap_ring.cpp -o build/tests-heap
	$(CXX) $(CXXFLAGS) $(SAN) tests/test_split.cpp -o build/tests-split
	./build/tests
	./build/tests-heap
	./build/tests-split

# ThreadSanitizer is the only one that can see a missing acquire or release.
# A lock-free queue that has not been through it has not been checked.
race:
	@mkdir -p build
	$(CXX) $(CXXFLAGS) -fsanitize=thread -O1 -g tests/test_spscring.cpp -o build/tests-tsan
	$(CXX) $(CXXFLAGS) -fsanitize=thread -O1 -g tests/test_heap_ring.cpp -o build/tests-heap-tsan
	$(CXX) $(CXXFLAGS) -fsanitize=thread -O1 -g tests/test_split.cpp -o build/tests-split-tsan
	./build/tests-tsan
	./build/tests-heap-tsan
	./build/tests-split-tsan

# The control for `race`: the same protocol correct, with the consumer's load
# weakened, and with the producer's store weakened. A clean `race` means
# something only if the weakened builds are reported, so this target fails
# unless both of them are. abort_on_error=0 is needed because TSan defaults to
# aborting on macOS, where a reported run dies with SIGABRT and exit 134
# instead of the exit code asked for.
race-control:
	@mkdir -p build
	$(CXX) $(CXXFLAGS) -fsanitize=thread -O1 -g tests/race_control.cpp -o build/race-correct
	$(CXX) $(CXXFLAGS) -fsanitize=thread -O1 -g -DSPSCRING_WEAKEN=load tests/race_control.cpp -o build/race-weak-load
	$(CXX) $(CXXFLAGS) -fsanitize=thread -O1 -g -DSPSCRING_WEAKEN=store tests/race_control.cpp -o build/race-weak-store
	@failed=0; \
	for variant in correct load store; do \
		case $$variant in \
			correct) binary=build/race-correct; expected=0 ;; \
			*) binary=build/race-weak-$$variant; expected=66 ;; \
		esac; \
		code=0; \
		TSAN_OPTIONS=abort_on_error=0:exitcode=66 ./$$binary || code=$$?; \
		if [ $$code -ne $$expected ]; then \
			echo "make race has no teeth: $$variant exited $$code"; \
			failed=1; \
		fi; \
	done; \
	[ $$failed -eq 0 ]

demo:
	@mkdir -p build
	$(CXX) $(CXXFLAGS) -O2 example/main.cpp -o build/demo
	./build/demo

clean:
	rm -rf build
