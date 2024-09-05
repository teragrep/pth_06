#!/bin/bash
find src/main/java/com/teragrep/pth_06/jooq/generated -type f -name "*.java" -print0 | while read -r -d $'\0' file
do
    if ! grep -q "Copyright (C) .* Suomen Kanuuna Oy" "${file}"; then
        cat license-header > "${file}.tmp";
        cat "${file}" >> "${file}.tmp";
        mv -f "${file}.tmp" "${file}";
    fi;
done
