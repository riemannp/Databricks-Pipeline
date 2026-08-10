#!/bin/bash
# Test Runner Script for Databricks Dynamic Pipeline
# Usage: ./run_tests.sh [option]

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "${GREEN}===========================================" 
echo "Databricks Pipeline - Test Suite"
echo "===========================================${NC}"

# Check if pytest is installed
if ! command -v pytest &> /dev/null; then
    echo "${RED}Error: pytest not found. Installing dependencies...${NC}"
    pip install -r requirements.txt
fi

# Parse command line arguments
OPTION=${1:-all}

case $OPTION in
    all)
        echo "${YELLOW}Running all tests with coverage...${NC}"
        pytest tests/ -v --cov=src --cov-report=html --cov-report=term-missing
        echo ""
        echo "${GREEN}✓ Coverage report generated: htmlcov/index.html${NC}"
        ;;
    
    fast)
        echo "${YELLOW}Running tests without coverage (fast mode)...${NC}"
        pytest tests/ -v
        ;;
    
    config)
        echo "${YELLOW}Running configuration tests...${NC}"
        pytest tests/test_config.py -v
        ;;
    
    transforms)
        echo "${YELLOW}Running transformation tests...${NC}"
        pytest tests/test_transforms.py -v
        ;;
    
    bronze)
        echo "${YELLOW}Running Bronze layer tests...${NC}"
        pytest tests/test_bronze_logic.py -v
        ;;
    
    silver)
        echo "${YELLOW}Running Silver layer tests...${NC}"
        pytest tests/test_silver_logic.py -v
        ;;
    
    gold)
        echo "${YELLOW}Running Gold layer tests...${NC}"
        pytest tests/test_gold_logic.py -v
        ;;
    
    coverage)
        echo "${YELLOW}Generating coverage report only...${NC}"
        pytest tests/ --cov=src --cov-report=html --cov-report=term-missing > /dev/null 2>&1
        echo "${GREEN}✓ Coverage report: htmlcov/index.html${NC}"
        echo ""
        echo "${YELLOW}Opening coverage report in browser...${NC}"
        if [[ "$OSTYPE" == "darwin"* ]]; then
            open htmlcov/index.html
        elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
            xdg-open htmlcov/index.html
        elif [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "win32" ]]; then
            start htmlcov/index.html
        fi
        ;;
    
    ci)
        echo "${YELLOW}Running CI/CD validation (strict mode)...${NC}"
        echo "1. Linting with Ruff..."
        ruff check src/ tests/ --ignore E501
        echo "${GREEN}✓ Linting passed${NC}"
        echo ""
        echo "2. Running tests with coverage..."
        pytest tests/ -v --cov=src --cov-report=term-missing --cov-fail-under=80
        echo "${GREEN}✓ All tests passed with 80%+ coverage${NC}"
        ;;
    
    watch)
        echo "${YELLOW}Running tests in watch mode...${NC}"
        echo "${YELLOW}Press Ctrl+C to stop${NC}"
        pytest-watch tests/ -- -v
        ;;
    
    help)
        echo "Usage: ./run_tests.sh [option]"
        echo ""
        echo "Options:"
        echo "  all         - Run all tests with coverage (default)"
        echo "  fast        - Run all tests without coverage"
        echo "  config      - Run only configuration tests"
        echo "  transforms  - Run only transformation tests"
        echo "  bronze      - Run only Bronze layer tests"
        echo "  silver      - Run only Silver layer tests"
        echo "  gold        - Run only Gold layer tests"
        echo "  coverage    - Generate and open coverage report"
        echo "  ci          - Run CI/CD validation (linting + tests + coverage check)"
        echo "  watch       - Run tests in watch mode (requires pytest-watch)"
        echo "  help        - Show this help message"
        echo ""
        echo "Examples:"
        echo "  ./run_tests.sh              # Run all tests"
        echo "  ./run_tests.sh fast         # Quick test run"
        echo "  ./run_tests.sh silver       # Test Silver layer only"
        echo "  ./run_tests.sh ci           # CI/CD validation"
        ;;
    
    *)
        echo "${RED}Error: Unknown option '$OPTION'${NC}"
        echo "Run './run_tests.sh help' for usage information"
        exit 1
        ;;
esac

echo ""
echo "${GREEN}==========================================="
echo "Test run completed!"
echo "===========================================${NC}"
