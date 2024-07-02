#include <unistd.h>
#include <string>
#include <fcntl.h>
#include <iostream>
#include <chrono>
#include <cstdlib>

int main() {
  std::string file_name("output.dat");
  int fd = open(file_name.c_str(), O_DIRECT | O_RDONLY);
  if (fd < 0) {
    std::cout << "fail to open file" << std::endl;
    exit(0);
  }
  int size = 512;
  char buf[512] = {0};
  std::chrono::nanoseconds total(0);
  for (int i = 0; i < 1000; i++) {
    auto t1 = std::chrono::high_resolution_clock::now();
    int offset = size * (rand() % 100);
    auto s = pread(fd, buf, size, offset);
    auto t2 = std::chrono::high_resolution_clock::now();
    auto ns_int = std::chrono::duration_cast<std::chrono::nanoseconds>(t2 - t1);
    total += ns_int;
    if (s < 0) {
      std::cout << "fail to read file: " << s << std::endl;
      exit(0);
    }
  }
  std::cout << (double)total.count() / 1000.0 / size << std::endl;
  return 0;
}