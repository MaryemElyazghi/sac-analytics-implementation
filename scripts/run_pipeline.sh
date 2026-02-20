#!/bin/bash
# =============================================================
# run_pipeline.sh
# Full SAC Analytics pipeline — generate → transform → validate
# → calculate KPIs → run tests
# =============================================================

set -e   # Exit on first error

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
BOLD='\033[1m'
RESET='\033[0m'

echo ""
echo -e "${BOLD}${CYAN}============================================================${RESET}"
echo -e "${BOLD}${CYAN}  SAC Analytics Implementation — Full Pipeline Execution${RESET}"
echo -e "${BOLD}${CYAN}============================================================${RESET}"
echo ""

# ── Step 1: Check Python ──────────────────────────────────────
echo -e "${YELLOW}[1/5] Checking Python environment...${RESET}"
python --version || { echo -e "${RED}Python not found. Install Python 3.10+${RESET}"; exit 1; }
echo -e "${GREEN}✅ Python OK${RESET}\n"

# ── Step 2: Install dependencies ─────────────────────────────
echo -e "${YELLOW}[2/5] Installing dependencies...${RESET}"
pip install -r requirements.txt -q
echo -e "${GREEN}✅ Dependencies installed${RESET}\n"

# ── Step 3: Generate sample data ─────────────────────────────
echo -e "${YELLOW}[3/5] Generating sample ERP data...${RESET}"
python etl/generate_sample_data.py
echo -e "${GREEN}✅ Sample data generated${RESET}\n"

# ── Step 4: Run ETL transform ─────────────────────────────────
echo -e "${YELLOW}[4/5] Running ETL transform pipeline...${RESET}"
python etl/transform.py
echo -e "${GREEN}✅ ETL transform complete${RESET}\n"

# ── Step 5: Run data validation ───────────────────────────────
echo -e "${YELLOW}[5/5] Running data quality validation...${RESET}"
python etl/validate.py
echo -e "${GREEN}✅ Validation passed${RESET}\n"

# ── Step 6: Calculate KPIs ────────────────────────────────────
echo -e "${YELLOW}[6/6] Calculating KPIs...${RESET}"
python kpis/kpi_calculator.py
echo -e "${GREEN}✅ KPIs calculated${RESET}\n"

# ── Step 7: Run test suite ────────────────────────────────────
echo -e "${YELLOW}[7/7] Running test suite...${RESET}"
pytest tests/ -v --tb=short
echo -e "${GREEN}✅ All tests passed${RESET}\n"

# ── Done ──────────────────────────────────────────────────────
echo -e "${BOLD}${GREEN}============================================================${RESET}"
echo -e "${BOLD}${GREEN}  ✅ Pipeline complete! Outputs in data/processed/${RESET}"
echo -e "${BOLD}${GREEN}============================================================${RESET}"
echo ""
echo "  Next steps:"
echo "  1. Upload data/processed/*.csv to SAP Analytics Cloud"
echo "  2. Import dashboards/story_config.json as Story template"
echo "  3. Configure refresh schedule in SAC data model"
echo ""
