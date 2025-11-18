# Main Makefile for Chapel OTF2 Projects
# Author: Generated Main Makefile
# Description: This Makefile does not build anything. Please cd into subdirectories to build.

# Subdirectories containing projects
SUBDIRS = simple read_events read_events_and_metrics trace_to_csv

# Default target - show help instead of building
.PHONY: all help

all:
	@echo "=========================================================================="
	@echo "  This top-level Makefile does not build anything."
	@echo "  Please navigate to a subdirectory and run 'make' there."
	@echo "=========================================================================="
	@echo ""
	@echo "Available subdirectories:"
	@for dir in $(SUBDIRS); do \
		echo "  - $$dir"; \
	done
	@echo ""
	@echo "Example usage:"
	@echo "  cd simple && make        # Build all targets in simple/"
	@echo "  cd simple && make serial # Build only serial version"
	@echo "  cd simple && make help   # Show help for that directory"
	@echo ""
	@echo "Run 'make help' for more information."

# Help target
help:
	@echo "=========================================================================="
	@echo "  Main Makefile for Chapel OTF2 Projects"
	@echo "=========================================================================="
	@echo ""
	@echo "This Makefile does not build targets. To build, you must:"
	@echo "  1. Navigate into a subdirectory"
	@echo "  2. Run 'make' or 'make <target>' there"
	@echo ""
	@echo "Available subdirectories:"
	@for dir in $(SUBDIRS); do \
		echo "  - $$dir"; \
	done
	@echo ""
	@echo "Example workflow:"
	@echo "  cd simple"
	@echo "  make help     # See available targets in that directory"
	@echo "  make all      # Build all available targets"
	@echo "  make serial   # Build only serial version"
	@echo "  make clean    # Clean built artifacts"
	@echo ""
	@echo "Each subdirectory has its own Makefile with appropriate targets."
	@echo "=========================================================================="
