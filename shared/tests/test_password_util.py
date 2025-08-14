import pytest
from shared.utils.password_util import derive_key, hash_password, verify_password


class TestDeriveKey:
    def test_derive_key_basic(self):
        """Test basic key derivation"""
        password = "test_password"
        salt = "test_salt"

        key = derive_key(password, salt)

        assert isinstance(key, bytes)
        assert len(key) == 64  # 512 bits / 8 = 64 bytes

    def test_derive_key_consistency(self):
        """Test that same password and salt produce same key"""
        password = "test_password"
        salt = "test_salt"

        key1 = derive_key(password, salt)
        key2 = derive_key(password, salt)

        assert key1 == key2

    def test_derive_key_different_passwords(self):
        """Test that different passwords produce different keys"""
        salt = "same_salt"

        key1 = derive_key("password1", salt)
        key2 = derive_key("password2", salt)

        assert key1 != key2

    def test_derive_key_different_salts(self):
        """Test that different salts produce different keys"""
        password = "same_password"

        key1 = derive_key(password, "salt1")
        key2 = derive_key(password, "salt2")

        assert key1 != key2

    def test_derive_key_empty_strings(self):
        """Test key derivation with empty strings"""
        key1 = derive_key("", "salt")
        key2 = derive_key("password", "")
        key3 = derive_key("", "")

        assert isinstance(key1, bytes)
        assert isinstance(key2, bytes)
        assert isinstance(key3, bytes)
        assert len(key1) == 64
        assert len(key2) == 64
        assert len(key3) == 64

    def test_derive_key_unicode(self):
        """Test key derivation with unicode characters"""
        password = "пароль🔐"
        salt = "соль🧂"

        key = derive_key(password, salt)

        assert isinstance(key, bytes)
        assert len(key) == 64


class TestHashPassword:
    def test_hash_password_with_salt(self):
        """Test password hashing with provided salt"""
        password = "test_password"
        salt = "test_salt"

        result_salt, hashed_password = hash_password(password, salt)

        assert isinstance(result_salt, str)
        assert isinstance(hashed_password, str)
        assert result_salt == salt
        assert len(hashed_password) == 128  # 64 bytes * 2 (hex)

    def test_hash_password_without_salt(self):
        """Test password hashing with auto-generated salt"""
        password = "test_password"

        result_salt, hashed_password = hash_password(password, None)

        assert isinstance(result_salt, str)
        assert isinstance(hashed_password, str)
        assert len(result_salt) == 32  # 16 bytes * 2 (hex)
        assert len(hashed_password) == 128  # 64 bytes * 2 (hex)

    def test_hash_password_empty_salt(self):
        """Test password hashing with empty salt string"""
        password = "test_password"

        result_salt, hashed_password = hash_password(password, "")

        assert isinstance(result_salt, str)
        assert isinstance(hashed_password, str)
        assert len(result_salt) == 32  # Auto-generated salt

    def test_hash_password_consistency(self):
        """Test that same password and salt produce same hash"""
        password = "test_password"
        salt = "test_salt"

        salt1, hash1 = hash_password(password, salt)
        salt2, hash2 = hash_password(password, salt)

        assert hash1 == hash2
        assert salt1 == salt2

    def test_hash_password_different_auto_salts(self):
        """Test that auto-generated salts are different"""
        password = "test_password"

        salt1, hash1 = hash_password(password, None)
        salt2, hash2 = hash_password(password, None)

        assert salt1 != salt2
        assert hash1 != hash2


class TestVerifyPassword:
    def test_verify_password_correct(self):
        """Test password verification with correct password"""
        password = "test_password"
        salt = "test_salt"

        result_salt, hashed_password = hash_password(password, salt)

        assert verify_password(password, salt, hashed_password) is True

    def test_verify_password_incorrect(self):
        """Test password verification with incorrect password"""
        password = "test_password"
        wrong_password = "wrong_password"
        salt = "test_salt"

        result_salt, hashed_password = hash_password(password, salt)

        assert verify_password(wrong_password, salt, hashed_password) is False

    def test_verify_password_wrong_salt(self):
        """Test password verification with wrong salt"""
        password = "test_password"
        salt = "test_salt"
        wrong_salt = "wrong_salt"

        result_salt, hashed_password = hash_password(password, salt)

        assert verify_password(password, wrong_salt, hashed_password) is False

    def test_verify_password_wrong_hash(self):
        """Test password verification with wrong hash"""
        password = "test_password"
        salt = "test_salt"
        wrong_hash = "0" * 128  # Invalid hash

        assert verify_password(password, salt, wrong_hash) is False

    def test_verify_password_empty_inputs(self):
        """Test password verification with empty inputs"""
        assert verify_password("", "", "") is False
        assert verify_password("password", "", "") is False
        assert verify_password("", "salt", "") is False
        assert verify_password("", "", "hash") is False


class TestPasswordWorkflow:
    def test_full_workflow_with_provided_salt(self):
        """Test complete workflow: hash then verify with provided salt"""
        password = "my_secure_password"
        salt = "my_salt"

        # Hash the password
        result_salt, hashed_password = hash_password(password, salt)

        # Verify the password
        is_valid = verify_password(password, result_salt, hashed_password)

        assert is_valid is True

    def test_full_workflow_with_auto_salt(self):
        """Test complete workflow: hash then verify with auto-generated salt"""
        password = "my_secure_password"

        # Hash the password with auto-generated salt
        result_salt, hashed_password = hash_password(password, None)

        # Verify the password
        is_valid = verify_password(password, result_salt, hashed_password)

        assert is_valid is True

    def test_workflow_with_wrong_password(self):
        """Test workflow with wrong password during verification"""
        password = "correct_password"
        wrong_password = "wrong_password"

        # Hash the correct password
        result_salt, hashed_password = hash_password(password, None)

        # Try to verify with wrong password
        is_valid = verify_password(wrong_password, result_salt, hashed_password)

        assert is_valid is False

    def test_workflow_multiple_users(self):
        """Test workflow with multiple users having same password"""
        password = "common_password"

        # Hash for user 1
        user1_salt, user1_hash = hash_password(password, None)

        # Hash for user 2
        user2_salt, user2_hash = hash_password(password, None)

        # Verify each user's password
        user1_valid = verify_password(password, user1_salt, user1_hash)
        user2_valid = verify_password(password, user2_salt, user2_hash)

        # Cross-verification should fail (different salts)
        cross_valid = verify_password(password, user1_salt, user2_hash)

        assert user1_valid is True
        assert user2_valid is True
        assert cross_valid is False
        assert user1_salt != user2_salt
        assert user1_hash != user2_hash


class TestEdgeCases:
    def test_very_long_password(self):
        """Test with very long password"""
        password = "a" * 1000
        salt = "test_salt"

        result_salt, hashed_password = hash_password(password, salt)
        is_valid = verify_password(password, salt, hashed_password)

        assert is_valid is True

    def test_special_characters(self):
        """Test with special characters in password"""
        password = "!@#$%^&*()_+-=[]{}|;':\",./<>?"
        salt = "test_salt"

        result_salt, hashed_password = hash_password(password, salt)
        is_valid = verify_password(password, salt, hashed_password)

        assert is_valid is True

    def test_unicode_password(self):
        """Test with unicode password"""
        password = "Пароль с эмодзи 🔐🔑🛡️"

        result_salt, hashed_password = hash_password(password, None)
        is_valid = verify_password(password, result_salt, hashed_password)

        assert is_valid is True
