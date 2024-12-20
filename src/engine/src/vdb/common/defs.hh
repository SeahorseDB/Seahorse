#pragma once

#include <array>
#include <cstdint>

namespace vdb {

constexpr std::array<uint8_t, 8> BitPos{0b00000001, 0b00000010, 0b00000100,
                                        0b00001000, 0b00010000, 0b00100000,
                                        0b01000000, 0b10000000};
constexpr std::array<uint8_t, 8> BitMask{0b11111111, 0b11111110, 0b11111100,
                                         0b11111000, 0b11110000, 0b11100000,
                                         0b11000000, 0b10000000};

constexpr char kRS = '\u001e';
constexpr char kGS = '\u001d';

constexpr const char *kCRLF = "\r\n";

constexpr int64_t kGb = (1 << 30);
}  // namespace vdb
