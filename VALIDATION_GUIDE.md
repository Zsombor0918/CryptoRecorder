# Validation & Testing Guide

Complete guide to validating and testing the CryptoRecorder system.

## Overview

CryptoRecorder includes a comprehensive validation system to ensure all components are working correctly:

- **VALIDATE.py** - Master validator (NEW - use this!) ⭐
- **validate_system.py** - Complete system validation (25 tests)
- **run_validators.py** - Alternative test runner with interactive menu
- **test_setup.py** - Setup and dependency verification

## Quick Start

### ⭐ Option 1: Master Validator (Recommended & Easiest)

```bash
cd ~/services/CryptoRecorder
source .venv/bin/activate
python3 VALIDATE.py all
```

Single command validates everything. Shows color-coded output and saves JSON report.

### Option 2: Interactive Menu

```bash
python3 VALIDATE.py
```

Menu-driven interface where you select what to validate.

### Option 3: Command Line (Fastest)

```bash
# Quick checks (30 seconds)
python3 VALIDATE.py quick

# Full validation (2 minutes)
python3 VALIDATE.py all

# Specific systems
python3 VALIDATE.py setup      # Dependencies only
python3 VALIDATE.py recorder   # Recorder modules
python3 VALIDATE.py converter  # Converter modules

# View last report
python3 VALIDATE.py report
```

### Alternative: Legacy run_validators.py

```bash
python3 run_validators.py quick
```

## Master Validator (VALIDATE.py)

The recommended validation tool - validates all systems in one command.

### Usage

```bash
python3 VALIDATE.py all
```

### Validation Coverage

- ✓ Setup & dependencies (17 checks)
- ✓ System validation (8 checks)
- ✓ Recorder system (3 modules)
- ✓ Converter system (3 modules)
- ✓ Disk management (2 modules)

**Total: 5 systems, 100% coverage**

### Output

```
► Setup & Dependencies Validation
✓ Setup validation passed (17/17 checks)

► System Validation
✓ System validation passed (8/8 quick checks)

► Recorder System Validation
✓ Recorder module imports successfully
✓ Storage module imports successfully
✓ Health monitor imports successfully

✓ All validations passed!
Report saved: state/master_validation_report.json
```

## Alternative: Legacy Validators

### run_validators.py

Interactive menu-driven test runner (older approach, still functional):

```bash
# Interactive menu
python3 run_validators.py

# Command line
python3 run_validators.py quick
python3 run_validators.py full
python3 run_validators.py all
```

### validate_system.py

Comprehensive system validation with detailed tests:

```bash
# Full validation with details
python3 validate_system.py --verbose

# Quick validation only
python3 validate_system.py --quick

# Auto-fix missing directories
python3 validate_system.py --fix
```

### test_setup.py

Setup and dependency verification:

```bash
python3 test_setup.py
```

## Validation Modes

### Quick Mode (30 seconds)

Fast checks of core dependencies:

```bash
python3 VALIDATE.py quick
```

**Validates:**
- ✓ Python dependencies installed
- ✓ System core checks

**Use when:** You just need to confirm basic setup is OK.

### Full Mode (2 minutes)

Complete system validation:

```bash
python3 VALIDATE.py all
```

**Validates:**
- ✓ All 17 setup checks
- ✓ All 8 system checks
- ✓ Recorder modules (3)
- ✓ Converter modules (3)
- ✓ Disk management (2)

**Use when:** You want comprehensive system verification.

### Component-Specific

Test individual systems:

```bash
python3 VALIDATE.py setup      # Setup only
python3 VALIDATE.py recorder   # Recorder modules
python3 VALIDATE.py converter  # Converter modules
```

**Use when:** You've made changes and want targeted validation.

## Understanding Results

### Successful Run

```
======================================================================
                      Validation Results Summary                      
======================================================================

  setup                ... PASS
  system               ... PASS
  recorder             ... PASS
  converter            ... PASS
  disk_management      ... PASS

Results:
  Total Validations: 5
  Passed: 5/5
  Failed: 0/5
  Success Rate: 100.0%
✓ All validations passed!
```

✅ **All systems ready!** Proceed to run recorder.

### Failures

```
✗ Converter modules
  → cannot import name 'LocalOrderBook' from 'converter.book_builder'
```

**Action:**
1. Check error message
2. Review the specific validation output
3. Check logs: `tail state/validation*.log`
4. Run setup check: `python3 VALIDATE.py setup`

## Test Reports

### Master Validation Report

Comprehensive test results saved to `state/master_validation_report.json`:

```bash
cat state/master_validation_report.json | python3 -m json.tool

# Output:
{
  "timestamp": "2026-04-17T11:56:19Z",
  "total_validations": 5,
  "passed": 5,
  "failed": 0,
  "results": {
    "setup": {"passed": true, "checks": 17, ...},
    "system": {"passed": true, "checks": 8, ...},
    "recorder": {"passed": true, "modules": 3, ...},
    "converter": {"passed": true, "modules": 3, ...},
    "disk_management": {"passed": true, "modules": 2, ...}
  }
}
```

### Legacy Reports

For legacy validators:

```bash
# From validate_system.py
cat state/validation_report.json | jq

# From test_setup.py
tail state/validation.log
```

## System Status

View system health and last validation results:

```bash
# Show last validation report
python3 VALIDATE.py report

# Show JSON details
cat state/master_validation_report.json | python3 -m json.tool
```

**Shows:**
- ✓ Validation timestamp
- ✓ All system pass/fail status
- ✓ Success rate percentage

## Auto-Fix Issues

Automatically create missing directories:

```bash
python3 validate_system.py --fix
```

**Creates:**
- state/ (state and metrics)
- data_raw/ (raw recordings)
- data_nautilus/ (converted data)
- meta/ (metadata cache)
- converter/ (converter modules)

## Setup Validation Phases

### Phase 1: Dependencies & Imports

Checks all Python packages are installed:
- asyncio, aiohttp, cryptofeed
- zstandard, yaml, pandas, pyarrow
- logging

**Common issues:**
- Missing: `pip install -r requirements.txt`
- Wrong Python version: Use Python 3.12+

### Phase 2: Configuration

Loads and validates config files:
- config.py - Main configuration
- config.yaml - YAML configuration (optional)

**What's checked:**
- VENUES defined
- TOP_SYMBOLS > 0
- Disk limits configured

### Phase 3: Core Modules

Validates all main modules:
- storage.StorageManager
- health_monitor.HealthMonitor
- binance_universe.UniverseSelector
- disk_monitor.DiskMonitor
- disk_plan.DiskPlanner

### Phase 4: Disk Management

Tests disk monitoring system:
- DiskMonitor initialization with config
- DiskPlanner plan generation
- Recommended settings validation

### Phase 5: Directories & Paths

Checks required directories:
- data_raw/ (raw data storage)
- data_nautilus/ (converted data)
- meta/ (metadata)
- state/ (monitoring state)
- converter/ (conversion modules)
- systemd/ (service files)

### Phase 6: Recorder Integration

Validates recorder.py:
- Global variables defined
- Required classes present
- Integration with disk monitor

### Phase 7: Converter System

Checks converter modules:
- converter.parsers (JSONLReader)
- converter.book_builder (LocalOrderBook)
- converter.nautilus_builder (builders)

### Phase 8: Async Functionality

Tests async/await support:
- asyncio available
- aiohttp working
- Event loop functionality

## Troubleshooting

### Validation Fails with Import Errors

```
✗ Import cryptofeed
  → No module named 'cryptofeed'
```

**Solution:**
```bash
pip install -r requirements.txt

# Then retry validation
python3 VALIDATE.py setup
```

### "Module not found" or "Cannot import" Errors

**Diagnose:**
```bash
# Check Python version
python3 --version  # Must be 3.12+

# Verify all requirements installed
pip list | grep -E "cryptofeed|aiohttp|zstandard|pandas|pyarrow"

# Test individual imports
python3 -c "import cryptofeed; print('✓ cryptofeed')"
python3 -c "import recorder; print('✓ recorder')"
```

**Fix:**
```bash
# Reinstall all dependencies
pip install -r requirements.txt --upgrade

# Run validation again
python3 VALIDATE.py all
```

### Configuration Loading Errors

```
✗ Load config
  → config not found or invalid
```

**Check:**
```bash
# Verify config.py exists
ls -la config.py config.yaml

# Test syntax
python3 -m py_compile config.py

# Load and print settings
python3 -c "import config; print(config.VENUES)"
```

### Disk or Directory Issues

```
✗ Directory validation
  → data_raw directory not found
```

**Fix:**
```bash
# Auto-create missing directories
python3 validate_system.py --fix

# Or manually
mkdir -p data_raw data_nautilus meta state
```

### Verbose Debugging

Get detailed error information:

```bash
# Run with full error details
python3 validate_system.py --verbose

# Run individual test
python3 VALIDATE.py recorder

# Check validation report
cat state/master_validation_report.json | python3 -m json.tool
```

## Automated Validation

### Continuous Monitoring

Create a cron job to validate daily:

```bash
# Edit crontab
crontab -e

# Add line:
0 6 * * * cd /home/zsom/services/CryptoRecorder && source .venv/bin/activate && python3 VALIDATE.py all > /tmp/validation.log 2>&1
```

### Post-Deployment Validation

After deploying new code:

```bash
#!/bin/bash
# validate_after_deploy.sh

cd ~/services/CryptoRecorder
source .venv/bin/activate

echo "Running validation..."
python3 VALIDATE.py all

if [ $? -eq 0 ]; then
    echo "✓ Validation passed - deploying service"
    sudo systemctl restart cryptofeed-recorder
else
    echo "✗ Validation failed - review logs"
    exit 1
fi
```

## Integration with CI/CD

Running validation in pipeline:

```bash
# GitHub Actions
- name: Validate CryptoRecorder
  run: |
    source .venv/bin/activate
    python validate_system.py
    test $? -eq 0

# Jenkins
stage('Validate') {
  steps {
    sh '''
      source .venv/bin/activate
      python validate_system.py
    '''
  }
}
```

## Performance

Typical validation times:

| Mode | Time | Tests |
|------|------|-------|
| Quick | 30s | 8 |
| Setup | 1m | 5 |
| Disk Plan | 5s | 2 |
| Full | 2m | 25 |
| All | 5m | 40 |

## Next Steps

After successful validation:

1. **Start Recording:**
   ```bash
   python recorder.py
   ```

2. **Monitor Progress:**
   ```bash
   tail -f state/heartbeat.json | jq
   ```

3. **Check Data:**
   ```bash
   ls -lh data_raw/BINANCE_SPOT/depth/*/$(date +%Y-%m-%d)/
   ```

4. **Setup Production:**
   ```bash
   sudo cp systemd/*.service systemd/*.timer /etc/systemd/system/
   sudo systemctl enable --now cryptofeed-recorder
   ```

## Files Reference

### Validation Scripts

- **validate_system.py** (400 lines) - Core validation engine
- **run_validators.py** (500 lines) - Interactive test runner
- **test_setup.py** - Setup and dependency checker

### Output Files

- **state/validation.log** - Detailed validation logs
- **state/validation_report.json** - JSON test results
- **state/disk_usage.json** - Disk metrics (if recorded)
- **state/disk_plan_report.json** - Retention plan (if generated)

## See Also

- [QUICKSTART.md](QUICKSTART.md) - Quick start guide
- [DISK_MANAGEMENT.md](DISK_MANAGEMENT.md) - Disk system
- [README.md](README.md) - Full documentation
- [config.py](config.py) - Configuration reference
