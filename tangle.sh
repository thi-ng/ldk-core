#!/bin/sh
# -*- mode: shell-script -*-
mkdir -p \
babel/src-clj babel/src-cljs \
babel/src-cljx/thi/ng/ldk/core \
babel/src-cljx/thi/ng/ldk/query \
babel/src-cljx/thi/ng/ldk/inference \
babel/src-cljx/thi/ng/ldk/io \
babel/src-cljx/thi/ng/ldk/store \
babel/src-cljx/thi/ng/ldk/common \
babel/resources/rules \
babel/resources/public/js

DIR=`pwd`
FILES=""

# wrap each argument in the code required to call tangle on it
for i in $@; do
    FILES="$FILES \"$i\""
done

/Applications/Emacs.app/Contents/MacOS/Emacs-10.7 -Q --batch \
    --eval "(progn
     (add-to-list 'load-path (expand-file-name \"~/.emacs.d/elpa/org-20130902/\"))
     (require 'org)(require 'ob)(require 'ob-tangle)
     (mapc (lambda (file)
            (find-file (expand-file-name file \"$DIR\"))
            (org-babel-tangle)
            (kill-buffer)) '($FILES)))" #2>&1 | grep Tangled