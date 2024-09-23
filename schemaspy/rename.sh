#!/bin/bash
grep "$1" -l -r $2 | xargs -d '\n' sed -i -e "s|${1}||g" -e "s|\.removethisfakeschema||g" -e "s|removethisfakeschema|main|g"