#!/bin/bash
###############################################################################
# Orchestrator Deployment Script for Linux
#
# This script automates the deployment of the Bionews orchestrator pipeline
# to a Linux environment.
#
# Usage:
#   ./deploy.sh [OPTIONS]
#
# Options:
#   --install-dir PATH    Installation directory (default: /opt/orchestrator)
#   --user USERNAME       User to run as (default: current user)
#   --venv                Create and use virtual environment (recommended)
#   --systemd             Set up systemd service
#   --cron                Set up cron jobs
#   --docker              Deploy using Docker
#   --help                Show this help message
#
# Example:
#   ./deploy.sh --install-dir /opt/orchestrator --venv --systemd
#
# Author: Bionews Data Team
# Version: 1.0.0
# Date: 2025-10-23
###############################################################################

set -e  # Exit on error

# Default configuration
INSTALL_DIR="/opt/orchestrator"
USE_VENV=false
SETUP_SYSTEMD=false
SETUP_CRON=false
USE_DOCKER=false
USER=$(whoami)

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

show_help() {
    head -n 30 "$0" | tail -n +3 | sed 's/^# //' | sed 's/^#//'
    exit 0
}

check_prerequisites() {
    log_info "Checking prerequisites..."

    # Check Python version
    if ! command -v python3 &> /dev/null; then
        log_error "Python 3 is not installed"
        exit 1
    fi

    PYTHON_VERSION=$(python3 --version | cut -d' ' -f2 | cut -d'.' -f1,2)
    if (( $(echo "$PYTHON_VERSION < 3.9" | bc -l) )); then
        log_error "Python 3.9+ is required (found $PYTHON_VERSION)"
        exit 1
    fi
    log_success "Python $PYTHON_VERSION found"

    # Check pip
    if ! command -v pip3 &> /dev/null; then
        log_error "pip3 is not installed"
        exit 1
    fi
    log_success "pip3 found"

    # Check git
    if ! command -v git &> /dev/null; then
        log_error "git is not installed"
        exit 1
    fi
    log_success "git found"
}

clone_repository() {
    log_info "Cloning repository to $INSTALL_DIR..."

    # Check if directory exists
    if [ -d "$INSTALL_DIR" ]; then
        log_warning "Directory $INSTALL_DIR already exists"
        read -p "Do you want to update it? (y/n) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            cd "$INSTALL_DIR"
            git pull origin main
            log_success "Repository updated"
        fi
    else
        # Create parent directory if needed
        PARENT_DIR=$(dirname "$INSTALL_DIR")
        if [ ! -d "$PARENT_DIR" ]; then
            sudo mkdir -p "$PARENT_DIR"
        fi

        # Clone repository
        sudo git clone https://github.com/bmacinnis/orchestrator.git "$INSTALL_DIR"
        sudo chown -R "$USER:$USER" "$INSTALL_DIR"
        log_success "Repository cloned"
    fi
}

setup_python_env() {
    log_info "Setting up Python environment..."

    cd "$INSTALL_DIR"

    if [ "$USE_VENV" = true ]; then
        log_info "Creating virtual environment..."
        python3 -m venv venv
        source venv/bin/activate
        log_success "Virtual environment created"
    fi

    log_info "Installing Python dependencies..."
    pip install --upgrade pip
    pip install -r requirements.txt
    log_success "Dependencies installed"
}

setup_configuration() {
    log_info "Setting up configuration..."

    cd "$INSTALL_DIR"

    # Create .env file if it doesn't exist
    if [ ! -f .env ]; then
        cp env.example .env
        log_warning ".env file created from template - YOU MUST EDIT IT!"
        log_warning "Edit $INSTALL_DIR/.env with your credentials"
    else
        log_info ".env file already exists"
    fi

    # Create gcp directory
    mkdir -p gcp
    log_info "Created gcp/ directory for service account key"
    log_warning "You must copy your GCP service account key to: $INSTALL_DIR/gcp/"

    # Create logs directory
    mkdir -p logs

    # Set permissions
    chmod 600 .env
    chmod -R 755 .

    log_success "Configuration setup complete"
}

setup_systemd_service() {
    log_info "Setting up systemd service..."

    VENV_PATH=""
    if [ "$USE_VENV" = true ]; then
        VENV_PATH="$INSTALL_DIR/venv/bin"
    else
        VENV_PATH="/usr/bin"
    fi

    # Create service file
    sudo tee /etc/systemd/system/orchestrator.service > /dev/null <<EOF
[Unit]
Description=Bionews Data Pipeline Orchestrator
After=network.target

[Service]
Type=simple
User=$USER
Group=$USER
WorkingDirectory=$INSTALL_DIR
Environment="PATH=$VENV_PATH:/usr/bin"
ExecStart=$VENV_PATH/python $INSTALL_DIR/orchestrate.py --source facebook --env prod
Restart=on-failure
RestartSec=10
StandardOutput=append:/var/log/orchestrator/orchestrator.log
StandardError=append:/var/log/orchestrator/orchestrator-error.log

[Install]
WantedBy=multi-user.target
EOF

    # Create log directory
    sudo mkdir -p /var/log/orchestrator
    sudo chown "$USER:$USER" /var/log/orchestrator

    # Reload systemd
    sudo systemctl daemon-reload

    log_success "Systemd service created"
    log_info "Enable with: sudo systemctl enable orchestrator.service"
    log_info "Start with: sudo systemctl start orchestrator.service"
}

setup_cron_jobs() {
    log_info "Setting up cron jobs..."

    VENV_ACTIVATE=""
    if [ "$USE_VENV" = true ]; then
        VENV_ACTIVATE="source $INSTALL_DIR/venv/bin/activate && "
    fi

    # Backup existing crontab
    crontab -l > /tmp/crontab_backup_$(date +%Y%m%d%H%M%S) 2>/dev/null || true

    # Add new cron jobs
    (crontab -l 2>/dev/null; cat <<EOF

# Bionews Orchestrator - Automated Pipelines
# Daily Facebook extraction at 2 AM
0 2 * * * cd $INSTALL_DIR && ${VENV_ACTIVATE}python orchestrate.py --source facebook --env prod >> /var/log/orchestrator/cron.log 2>&1

# Daily WordPress extraction at 3 AM
0 3 * * * cd $INSTALL_DIR && ${VENV_ACTIVATE}python orchestrate.py --source wordpress --env prod >> /var/log/orchestrator/cron.log 2>&1

# Weekly cleanup on Sundays at 4 AM
0 4 * * 0 cd $INSTALL_DIR && ${VENV_ACTIVATE}python pipeline_manager.py cleanup --force --job-log-days 90 >> /var/log/orchestrator/cleanup.log 2>&1

# Daily health check at 8 AM
0 8 * * * cd $INSTALL_DIR && ${VENV_ACTIVATE}python pipeline_manager.py health --hours-back 24 >> /var/log/orchestrator/health.log 2>&1

# Source reliability scorecard + degradation alert at 7 AM (after the nightly suite).
# Standalone read-only report (no --source), so it is a separate cron line rather than
# an etl-suite step (every suite step must include --source). Emails when a source's
# reliability degrades below floor or drops sharply vs its 30-day baseline.
0 7 * * * cd $INSTALL_DIR && ${VENV_ACTIVATE}python orchestrate.py --env prod --scorecard --scorecard-alert >> /var/log/orchestrator/scorecard.log 2>&1
EOF
    ) | crontab -

    log_success "Cron jobs configured"
    log_info "View with: crontab -l"
}

docker_deployment() {
    log_info "Setting up Docker deployment..."

    cd "$INSTALL_DIR"

    # Check if Docker is installed
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed"
        exit 1
    fi

    # Build Docker image
    log_info "Building Docker image..."
    docker build -t bionews/orchestrator:latest .

    log_success "Docker image built"
    log_info "Run with: docker run -d --name orchestrator --env-file .env -v $INSTALL_DIR/gcp:/app/gcp:ro bionews/orchestrator:latest"
}

run_tests() {
    log_info "Running tests..."

    cd "$INSTALL_DIR"

    if [ "$USE_VENV" = true ]; then
        source venv/bin/activate
    fi

    # Syntax tests
    python -m py_compile orchestrate.py
    python -m py_compile pipeline_manager.py

    # Validation test
    python orchestrate.py --source facebook --env prod --validate-only

    log_success "Tests passed"
}

show_next_steps() {
    echo ""
    log_success "Deployment complete!"
    echo ""
    echo "========================================="
    echo "Next Steps:"
    echo "========================================="
    echo ""
    echo "1. Edit configuration:"
    echo "   nano $INSTALL_DIR/.env"
    echo ""
    echo "2. Copy GCP service account key:"
    echo "   scp service-account.json $USER@$(hostname):$INSTALL_DIR/gcp/"
    echo ""
    echo "3. Test extraction:"
    if [ "$USE_VENV" = true ]; then
        echo "   cd $INSTALL_DIR && source venv/bin/activate"
    else
        echo "   cd $INSTALL_DIR"
    fi
    echo "   python orchestrate.py --source facebook --env prod --validate-only"
    echo ""
    if [ "$SETUP_SYSTEMD" = true ]; then
        echo "4. Start systemd service:"
        echo "   sudo systemctl enable orchestrator.service"
        echo "   sudo systemctl start orchestrator.service"
        echo "   sudo systemctl status orchestrator.service"
        echo ""
    fi
    if [ "$SETUP_CRON" = true ]; then
        echo "5. View cron jobs:"
        echo "   crontab -l"
        echo ""
    fi
    echo "Documentation:"
    echo "   $INSTALL_DIR/README.md"
    echo "   $INSTALL_DIR/LINUX_DEPLOYMENT_GUIDE.md"
    echo "   $INSTALL_DIR/PIPELINE_MANAGER_MANUAL.md"
    echo ""
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --install-dir)
            INSTALL_DIR="$2"
            shift 2
            ;;
        --user)
            USER="$2"
            shift 2
            ;;
        --venv)
            USE_VENV=true
            shift
            ;;
        --systemd)
            SETUP_SYSTEMD=true
            shift
            ;;
        --cron)
            SETUP_CRON=true
            shift
            ;;
        --docker)
            USE_DOCKER=true
            shift
            ;;
        --help)
            show_help
            ;;
        *)
            log_error "Unknown option: $1"
            show_help
            ;;
    esac
done

# Main deployment flow
main() {
    echo "========================================="
    echo "Orchestrator Deployment Script"
    echo "========================================="
    echo ""

    check_prerequisites

    if [ "$USE_DOCKER" = true ]; then
        clone_repository
        setup_configuration
        docker_deployment
    else
        clone_repository
        setup_python_env
        setup_configuration
        run_tests

        if [ "$SETUP_SYSTEMD" = true ]; then
            setup_systemd_service
        fi

        if [ "$SETUP_CRON" = true ]; then
            setup_cron_jobs
        fi
    fi

    show_next_steps
}

# Run main function
main
