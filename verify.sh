#!/bin/bash
set -e
pep8 --max-line-length=120 haproxy.py
py.test test_haproxy.py
