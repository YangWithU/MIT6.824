#!/bin/bash

input_file="new_log.txt"
output_file="sorted_${input_file}"

# 使用sort排序
sort -k1.1,1.10 "$input_file" > "$output_file"

echo "Sorted file saved as $output_file"