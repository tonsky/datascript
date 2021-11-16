#!/bin/bash

clj -A:test:bench:datomic:nrepl -M -m nrepl.cmdline --interactive