# Copyright Hewlett Packard Enterprise Development LP.

# Makefile for Read Events and Metrics Chapel OTF2 Reader
# Description: Compiles read_events_and_metrics.chpl (serial version only)

# Include common variables and rules
include ../Makefile.common

# Set the default goal explicitly
.DEFAULT_GOAL := all

# ============================================================================
# Project Configuration
# ============================================================================

# Base name for the project
BASE_NAME = read_events_and_metrics

# Chapel OTF2 module directory (relative to this Makefile)
CHPL_OTF2_MODULE_DIR = ../_chpl

# ============================================================================
# Source Files and Targets
# ============================================================================

# Define source files that actually exist
SERIAL_SOURCE = $(BASE_NAME).chpl

# Define target executables (only for files that exist)
SERIAL_TARGET = $(BASE_NAME)

# All targets - only serial version exists
ALL_TARGETS = $(SERIAL_TARGET)

# ============================================================================
# Phony Targets
# ============================================================================

.PHONY: all clean help rebuild serial

# ============================================================================
# Build Targets
# ============================================================================

# Default target - build all available versions
all: $(ALL_TARGETS)

# Individual build rule for serial version
$(SERIAL_TARGET): $(SERIAL_SOURCE)
	@$(MAKE) build-version \
		SOURCE_FILE=$< \
		TARGET=$@ \
		CHPL_OTF2_MODULE_DIR=$(CHPL_OTF2_MODULE_DIR)

# Version-specific convenience target
serial: $(SERIAL_TARGET)

# ============================================================================
# Clean and Rebuild
# ============================================================================

# Clean build artifacts
clean:
	@$(MAKE) clean-targets TARGETS="$(ALL_TARGETS)"

# Force rebuild
rebuild: clean all

# ============================================================================
# Help
# ============================================================================

help:
	@echo "=========================================================================="
	@echo "  Makefile for Read Events and Metrics Chapel OTF2 Reader"
	@echo "=========================================================================="
	@echo ""
	@echo "Available targets:"
	@echo "  all          - Compile the serial version (default)"
	@echo "  serial       - Compile serial version"
	@echo "  clean        - Remove build artifacts"
	@echo "  rebuild      - Clean and rebuild"
	@echo "  help         - Show this help message"
	@echo ""
	@echo "Available source files:"
	@echo "  Serial:      $(SERIAL_SOURCE)"
	@echo ""
	@echo "Target executables:"
	@echo "  Serial:      $(SERIAL_TARGET)"
	@echo ""
	@$(MAKE) help-common CHPL_OTF2_MODULE_DIR=$(CHPL_OTF2_MODULE_DIR)
	@echo "=========================================================================="
