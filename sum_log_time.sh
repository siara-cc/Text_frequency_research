grep -a "\ u0 t.*" -o $1 | awk -F 't' "{ sum += \$2 } END { print sum/60/60 }"
