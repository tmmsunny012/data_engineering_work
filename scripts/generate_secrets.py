"""
Generate Secure Secrets for Environment Variables

This script generates cryptographically secure secrets for your .env file.

Usage:
    python scripts/generate_secrets.py
"""

import secrets
import string
from cryptography.fernet import Fernet


def generate_password(length=20):
    """Generate a secure random password."""
    chars = string.ascii_letters + string.digits + string.punctuation
    # Ensure at least one of each character type
    password = [
        secrets.choice(string.ascii_uppercase),
        secrets.choice(string.ascii_lowercase),
        secrets.choice(string.digits),
        secrets.choice(string.punctuation),
    ]
    # Fill the rest randomly
    password += [secrets.choice(chars) for _ in range(length - 4)]
    # Shuffle to avoid predictable patterns
    secrets.SystemRandom().shuffle(password)
    return ''.join(password)


def generate_fernet_key():
    """Generate Fernet encryption key for Airflow."""
    return Fernet.generate_key().decode()


def generate_secret_key(length=32):
    """Generate URL-safe secret key for Flask/Airflow sessions."""
    return secrets.token_urlsafe(length)


def main():
    print("=" * 80)
    print("SECURE SECRETS GENERATOR")
    print("=" * 80)
    print()
    print("Copy these values to your .env file:")
    print()
    print("-" * 80)

    print("\n# PostgreSQL Credentials")
    print(f"POSTGRES_PASSWORD={generate_password(20)}")

    print("\n# Airflow Security Keys")
    print(f"AIRFLOW__CORE__FERNET_KEY={generate_fernet_key()}")
    print(f"AIRFLOW__WEBSERVER__SECRET_KEY={generate_secret_key(32)}")

    print("\n# Airflow Admin Credentials")
    print(f"AIRFLOW_ADMIN_PASSWORD={generate_password(16)}")

    print("\n# pgAdmin Credentials")
    print(f"PGADMIN_DEFAULT_PASSWORD={generate_password(16)}")

    print()
    print("-" * 80)
    print()
    print("SECURITY NOTES:")
    print("1. These secrets are cryptographically secure")
    print("2. Store them safely (password manager, encrypted vault)")
    print("3. NEVER commit .env file to Git")
    print("4. Rotate secrets regularly in production")
    print("5. Use different secrets for dev/staging/prod")
    print()
    print("=" * 80)
    print()


if __name__ == "__main__":
    main()
