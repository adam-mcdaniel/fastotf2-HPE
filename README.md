# FastOTF2

A high-performance Chapel-based library for reading and processing OTF2 (Open Trace Format 2) trace files at scale. FastOTF2 provides native Chapel bindings for OTF2, enabling efficient parallel and distributed analysis of large-scale HPC application traces.

## Repository Structure

### Core Implementation
- **`chpl/`** - Main Chapel OTF2 processing library and tools
  - Chapel `OTF2` module for OTF2 reading (see in `_chpl`)
  - Example programs and utilities (`simple`, `read_events`, `read_events_and_metrics`, `trace_to_csv`)
  - Multiple implementation variants (serial, parallel, distributed) for different examples

- **`c`** - C versions of the same benchmarks, except `trace_to_csv`

### Development Environment
- **`container`** - Docker containers coming soon

### Building from Source

#### Prerequisites
- Chapel compiler (≥ 2.0.0)
- OTF2 library (≥ 3.0.0)
- GCC/Clang with C++14 support
- Make

#### Installation Steps

1. **Install Chapel**
   ```bash
   # Download and install Chapel from https://chapel-lang.org
   export CHPL_HOME=/path/to/chapel
   export PATH=$CHPL_HOME/bin:$PATH
   ```

2. **Install OTF2**
   ```bash
   # If using system package manager
   sudo apt-get install libotf2-dev  # Ubuntu/Debian
   # OR build from source in otf2-3.1.1/
   ```

3. **Build FastOTF2**
   ```bash
   cd chpl
   cd trace_to_csv # or whatever example you're trying to build
   make
   ```

## Usage

### Basic Usage

Refer to the **Makefile** in `chpl/` for comprehensive build targets and usage examples:

## Performance

FastOTF2 is designed for high-performance analysis of large trace files:

- **Parallel Processing**: Utilizes Chapel's task parallelism for multi-core efficiency
- TODO **Distributed Execution**: Scales across multiple nodes using Chapel's distributed arrays
- TODO **Memory Optimization**: Efficient memory usage patterns for large traces
- TODO **I/O Optimization**: Optimized reading patterns for OTF2 files

## Chapel OTF2 Module API

The core Chapel module aims to provides a 1:1 mapping to the C otf2
api in most places.
It is a work in progress.
See the readme in `chpl/_chpl`