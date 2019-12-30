# Makefile for Synerex.

GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
RM=rm


# Main target

.PHONY: build 
build: nodeserv

nodeserv: nodeserv.go
	$(GOBUILD)

.PHONY: clean
clean: 
	$(RM) nodeserv



