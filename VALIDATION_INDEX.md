# CryptoRecorder - Executable & Testing Quick Index

## 🎯 What You Just Got

A **production-ready, self-validating system** with comprehensive testing infrastructure, cleaned-up codebase, and user-friendly validation tools.

---

## ⚡ Start Here (Pick One)

### Option 1: Interactive Menu (Easiest)
```bash
python run_validators.py
```
**What it does:** Menu-driven interface where you select what to validate
**Time:** 30 seconds to 5 minutes depending on selection
**Best for:** First-time users, quick checks

### Option 2: One-Command Validation
```bash
python run_validators.py quick
```
**What it does:** Validates all core components
**Time:** ~30 seconds
**Best for:** Rapid system checks

### Option 3: Full Comprehensive Check
```bash
python validate_system.py
```
**What it does:** Runs all 25 tests in 8 phases
**Time:** ~2 minutes
**Best for:** Production validation, debugging

---

## 📋 Core Executables

### Validation Tools
| Script | Purpose | Time | Command |
|--------|---------|------|---------|
| `run_validators.py` | Interactive test menu | 30s-5m | `python run_validators.py` |
| `validate_system.py` | Comprehensive validation | 2m | `python validate_system.py` |
| `test_setup.py` | Setup verification | 1m | `python test_setup.py` |
| `disk_plan.py` | Retention planning | 5s | `python disk_plan.py` |

### Main Application
| Script | Purpose | Command |
|--------|---------|---------|
| `recorder.py` | Start data recording | `python recorder.py` |
| `convert_yesterday.py` | Convert daily data | `python convert_yesterday.py` |

---

## 📊 Understanding the Validation Results

### What Gets Tested (25 Tests in 8 Phases)

**Phase 1: Dependencies (8 tests)**
- ✓ asyncio, aiohttp, cryptofeed, zstandard, yaml, pandas, pyarrow, logging

**Phase 2: Configuration (2 tests)**
- ✓ config.py loads correctly
- ✓ config.yaml is valid

**Phase 3: Core Modules (5 tests)**
- ✓ Storage manager
- ✓ Health monitor
- ✓ Universe selector
- ✓ Disk monitor
- ✓ Disk planner

**Phase 4-8: Integration Tests (8 tests)**
- ✓ Disk management functionality
- ✓ Directory structure
- ✓ Recorder integration
- ✓ Converter modules
- ✓ Async/await functionality

### Expected Results

```
✓ All 25 tests pass
✓ Success rate: 100.0%
✓ No errors or warnings
```

---

## 🔍 Interpreting Output

### Successful Validation
```
======================================================================
VALIDATION SUMMARY
======================================================================
Total Tests: 25
Passed: 25 ✓
Failed: 0 ✗
Success Rate: 100.0%
```

### If Issues Found
```
FAILED: Some specific test
ERROR: Description of what went wrong
ACTION: python run_validators.py help  (for solutions)
```

### Auto-Fix Mode (Creates missing directories)
```
python validate_system.py --fix
```

---

## 📁 Generated Output Files

All validation outputs go to the `state/` directory:

```
state/
├── validation.log              # Detailed logs with timestamps
├── validation_report.json      # Test results (machine-readable)
├── disk_plan_report.json       # Disk retention recommendations
├── disk_usage.json             # Latest disk metrics
├── heartbeat.json              # Real-time system status
└── metrics.json                # Performance metrics
```

### Viewing Results

**Quick view:**
```bash
python run_validators.py results
```

**Detailed view:**
```bash
cat state/validation_report.json | jq
```

**Follow logs in real-time:**
```bash
tail -f state/validation.log
```

---

## 🗂️ File Structure After Cleanup

### Before (Messy)
```
❌ recorder_old.py            → Obsolete
❌ main.py                    → Old code
❌ converter.py               → Wrong location
❌ validate_*.py (3 files)    → Duplicate validators
❌ __pycache__/               → Cache
❌ .pytest_cache/             → Test cache
```

### After (Clean Production-Ready)
```
✅ CryptoRecorder/
   ├── Core: recorder.py, storage.py, health_monitor.py
   ├── Disk: disk_monitor.py, disk_plan.py
   ├── Converter: convert_yesterday.py, converter/ (modules)
   ├── Validation: validate_system.py, run_validators.py, test_setup.py
   ├── Documentation: 9 .md files (README, QUICKSTART, VALIDATION_GUIDE, etc.)
   ├── Config: config.py, config.yaml, requirements.txt
   └── Services: systemd/ (service files)
```

**Result:** Clean, maintainable, professional structure

---

## 🚀 Quick Deployment Workflow

### 1. Validate Everything Works
```bash
# Quick check
python run_validators.py quick

# Output should show: ✓ All tests pass
```

### 2. Generate Disk Plan
```bash
python disk_plan.py

# Output shows recommended settings for config.py
```

### 3. Start Recording
```bash
# Test mode (manual)
python recorder.py

# Production mode (systemd service)
sudo systemctl start cryptofeed-recorder
```

### 4. Monitor Progress
```bash
# Real-time metrics
tail -f state/heartbeat.json | jq

# Check system status
python run_validators.py status

# View validation results
python run_validators.py results
```

---

## 📚 Documentation Roadmap

| Document | Purpose | Read Time | Audience |
|----------|---------|-----------|----------|
| **QUICKSTART.md** | Setup & first run | 5 min | Everyone |
| **VALIDATION_GUIDE.md** | Testing documentation | 15 min | Operators |
| **VALIDATION_SUMMARY.md** | What was done in cleanup | 10 min | Everyone |
| **README.md** | Full documentation | 20 min | Developers |
| **DISK_MANAGEMENT.md** | Disk system details | 10 min | DevOps |
| **IMPLEMENTATION_SUMMARY.md** | Technical architecture | 15 min | Developers |

---

## 🎓 Learn by Example

### Example 1: First-Time Validation
```bash
$ python run_validators.py
# Follow the interactive menu
# Select "quick" or "full"
# View results
```

### Example 2: Production Validation
```bash
$ python validate_system.py
# Runs 25 tests
# Generates report in state/validation_report.json
# Shows summary with ✓ pass/✗ fail count
```

### Example 3: Troubleshooting
```bash
$ python run_validators.py help
# Shows common issues
# Offers solutions
# Provides debug commands
```

### Example 4: Disk Planning
```bash
$ python disk_plan.py
# Calculates retention based on disk size and growth rate
# Recommends DISK_SOFT_LIMIT_GB, DISK_HARD_LIMIT_GB, RAW_RETENTION_DAYS
# Outputs to state/disk_plan_report.json
```

---

## ✅ Checklist Before Going Live

- [ ] Run `python run_validators.py quick` → All tests pass ✓
- [ ] Run `python disk_plan.py` → Plan generated ✓
- [ ] Review disk recommendations in console output
- [ ] Manually verify `data_raw/` directory exists
- [ ] Check `/mnt/disk0/` has at least 50GB free
- [ ] Review `README.md` for configuration options
- [ ] Test `python recorder.py` for 5 minutes manually
- [ ] Deploy systemd service: `sudo systemctl enable cryptofeed-recorder`
- [ ] Monitor first 24 hours: `tail -f state/heartbeat.json | jq`

---

## 🔧 Advanced Use Cases

### Continuous Validation (Cron Job)
```bash
# Add to crontab to validate daily
0 2 * * * cd /path/to/CryptoRecorder && python validate_system.py >> state/cron_validation.log 2>&1
```

### CI/CD Integration
```bash
# GitHub Actions example
- name: Validate System
  run: python validate_system.py
```

### Adding Custom Tests
Edit `validate_system.py` and add your test function to the appropriate phase.

---

## 📞 Common Commands Cheat Sheet

```bash
# Validation
python run_validators.py                    # Interactive menu
python run_validators.py quick              # Fast check
python validate_system.py                   # Full validation
python validate_system.py --verbose         # Debug mode
python validate_system.py --fix             # Auto-create dirs

# Results
python run_validators.py results            # Show last results
python run_validators.py status             # System health
grep ERROR state/validation.log             # Find problems

# Disk Management
python disk_plan.py                         # Generate plan
ls -lh state/disk_plan_report.json         # View recommendations

# Testing
python test_setup.py                       # Setup verification
python recorder.py                         # Start recording manually
python convert_yesterday.py                # Manual conversion

# Monitoring
tail -f state/heartbeat.json | jq         # Real-time metrics
watch -n 5 'python run_validators.py status' # Refresh status

# Help
python run_validators.py help             # Show help menu
cat VALIDATION_GUIDE.md                   # Full testing guide
cat .validation_quick_reference            # This file
```

---

## 🎯 What Success Looks Like

### First Validation Run
```
✓ All 25 tests pass
✓ Success rate: 100.0%
✓ No errors reported
✓ state/validation_report.json created
→ System is ready to use!
```

### After Starting Recorder
```
✓ Data files appearing in data_raw/
✓ Metrics updating in state/heartbeat.json
✓ Regular validation checks passing
→ System is operating correctly!
```

### Daily Operations
```
✓ Quick validation passes
✓ No disk warnings
✓ Conversion running on schedule
→ Everything is working perfectly!
```

---

## 🚨 If Something Breaks

### Step 1: Quick Diagnosis
```bash
python run_validators.py quick
```

### Step 2: Detailed Check
```bash
python run_validators.py full
```

### Step 3: Read the Guide
```bash
python run_validators.py help
# or
cat VALIDATION_GUIDE.md
```

### Step 4: Auto-Fix (if possible)
```bash
python validate_system.py --fix
```

### Step 5: Check Logs
```bash
tail -100 state/validation.log
```

---

## Summary

| What | How | Result |
|-----|-----|--------|
| **Validate system** | `python run_validators.py` | 25/25 tests pass ✓ |
| **Understand results** | `python run_validators.py results` | View JSON report |
| **Plan disk usage** | `python disk_plan.py` | Get recommendations |
| **Fix issues** | `python validate_system.py --fix` | Auto-create dirs |
| **Run system** | `python recorder.py` | Start collecting data |
| **Troubleshoot** | `python run_validators.py help` | Get solutions |

---

**🎉 System is production-ready and fully self-validating!**

For detailed information, see `VALIDATION_GUIDE.md` or `QUICKSTART.md`.
