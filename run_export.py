#!/usr/bin/env python3
"""
Wrapper script to ensure all required packages are installed before running export
"""

import subprocess
import sys
import os
from pathlib import Path

def check_and_install_packages():
    """Check if required packages are installed and install them if needed"""
    
    required_packages = {
        'gql': 'gql[all]>=3.5.0',
        'dotenv': 'python-dotenv>=1.0.0',
        'pandas': 'pandas>=2.0.0',
        'requests': 'requests>=2.31.0',
        'google.ads.googleads': 'google-ads>=24.1.0',
        'google_auth_oauthlib': 'google-auth-oauthlib>=1.2.0',
        'google_auth_httplib2': 'google-auth-httplib2>=0.2.0'
    }
    
    missing_packages = []
    
    # Check each package
    for module_name, package_spec in required_packages.items():
        try:
            if '.' in module_name:
                # For nested modules like google.ads.googleads
                parts = module_name.split('.')
                __import__(parts[0])
                module = sys.modules[parts[0]]
                for part in parts[1:]:
                    module = getattr(module, part)
            else:
                __import__(module_name)
        except ImportError:
            print(f"❌ Missing package: {package_spec}")
            missing_packages.append(package_spec)
    
    if missing_packages:
        print("\n📦 Installing missing packages...")
        
        # Detect if we're using a virtual environment
        venv_path = Path(__file__).parent / 'venv'
        if venv_path.exists():
            pip_cmd = str(venv_path / 'bin' / 'pip')
        else:
            pip_cmd = sys.executable + ' -m pip'
        
        # Install missing packages
        for package in missing_packages:
            print(f"Installing {package}...")
            try:
                subprocess.check_call(f"{pip_cmd} install '{package}'", shell=True)
                print(f"✅ Successfully installed {package}")
            except subprocess.CalledProcessError as e:
                print(f"❌ Failed to install {package}: {e}")
                print("\nPlease install packages manually:")
                print(f"  {pip_cmd} install -r requirements.txt")
                return False
    else:
        print("✅ All required packages are installed")
    
    return True

def main():
    """Main function to run the export after checking dependencies"""
    
    print("🔍 Checking dependencies...")
    
    # Check and install packages if needed
    if not check_and_install_packages():
        print("\n❌ Failed to install required packages. Please install them manually.")
        sys.exit(1)
    
    print("\n🚀 Starting export...")
    
    # Import and run the export
    try:
        from export_orders import main as export_main
        export_main()
    except Exception as e:
        print(f"\n❌ Error running export: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()