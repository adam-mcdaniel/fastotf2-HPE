# Copyright Hewlett Packard Enterprise Development LP.

# Makefile for Read Events Chapel OTF2 Reader (Multiple Versions)
# Description: Compiles available versions of otf2_read_events.chpl

# Include common variables and rules
include ../Makefile.common

# Set the default goal explicitly
.DEFAULT_GOAL := all

# ============================================================================
# Project Configuration
# ============================================================================

# Base name for the project
BASE_NAME = otf2_read_events

# Chapel OTF2 module directory (relative to this Makefile)
CHPL_OTF2_MODULE_DIR = ../_chpl

# ============================================================================
# Source Files and Targets
# ============================================================================

# Define source files that actually exist
SERIAL_SOURCE = $(BASE_NAME).chpl
PARALLEL_SOURCE = $(BASE_NAME)_parallel.chpl
DISTRIBUTED_SOURCE = $(BASE_NAME)_distributed.chpl

# Define target executables (only for files that exist)
SERIAL_TARGET = $(BASE_NAME)
PARALLEL_TARGET = $(BASE_NAME)_parallel
DISTRIBUTED_TARGET = $(BASE_NAME)_distributed

# All targets - only include what we can actually build
ALL_TARGETS = $(SERIAL_TARGET) $(PARALLEL_TARGET) $(DISTRIBUTED_TARGET)

# ============================================================================
# Phony Targets
# ============================================================================

.PHONY: all clean help rebuild serial parallel distributed

# ============================================================================
# Build Targets
# ============================================================================

# Default target - build all available versions
all: $(ALL_TARGETS)

# Individual build rules for each version
$(SERIAL_TARGET): $(SERIAL_SOURCE)
	@$(MAKE) build-version \
		SOURCE_FILE=$< \
		TARGET=$@ \
		CHPL_OTF2_MODULE_DIR=$(CHPL_OTF2_MODULE_DIR)

$(PARALLEL_TARGET): $(PARALLEL_SOURCE)
	@$(MAKE) build-version \
		SOURCE_FILE=$< \
		TARGET=$@ \
		CHPL_OTF2_MODULE_DIR=$(CHPL_OTF2_MODULE_DIR)

$(DISTRIBUTED_TARGET): $(DISTRIBUTED_SOURCE)
	@$(MAKE) build-version \
		SOURCE_FILE=$< \
		TARGET=$@ \
		CHPL_OTF2_MODULE_DIR=$(CHPL_OTF2_MODULE_DIR)

# Version-specific convenience targets
serial: $(SERIAL_TARGET)

parallel: $(PARALLEL_TARGET)

distributed: $(DISTRIBUTED_TARGET)

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
	@echo "  Makefile for Read Events Chapel OTF2 Reader"
	@echo "=========================================================================="
	@echo ""
	@$(MAKE) help-versions
	@echo ""
	@echo "Available source files:"
	@echo "  Serial:      $(SERIAL_SOURCE)"
	@echo "  Parallel:    $(PARALLEL_SOURCE)"
	@echo "  Distributed: $(DISTRIBUTED_SOURCE)"
	@echo ""
	@echo "Target executables:"
	@echo "  Serial:      $(SERIAL_TARGET)"
	@echo "  Parallel:    $(PARALLEL_TARGET)"
	@echo "  Distributed: $(DISTRIBUTED_TARGET)"
	@echo ""
	@$(MAKE) help-common CHPL_OTF2_MODULE_DIR=$(CHPL_OTF2_MODULE_DIR)
	@echo "=========================================================================="
