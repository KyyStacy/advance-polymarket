import sys
import time

try:
    import polymarket_hft
    print("✅ Rust module 'polymarket_hft' found and loaded successfully!")
    
    # Simple check of classes with new EIP-712 signing arguments
    dummy_hex = "0x" + "0" * 64
    creds = polymarket_hft.RustApiCreds(
        "test_key", "test_secret", "test_pass",
        "137",          # chain_id
        dummy_hex,      # private_key
        dummy_hex,      # app_domain_ctf
        dummy_hex,      # app_domain_neg_risk
        dummy_hex,      # order_type_hash
        dummy_hex,      # solady_type_hash
        dummy_hex,      # wallet_name_hash
        dummy_hex,      # wallet_version_hash
        dummy_hex,      # wallet_salt
        "00" * 32,      # contents_type_suffix
        "0x" + "0" * 40 # signer_address
    )
    pool = polymarket_hft.RustHttpPool("https://clob.polymarket.com", 1100)
    
    print(f"✅ Class instantiation works: {creds}")
    print(f"✅ Pool initialization works: {pool}")
    
    print("\n🚀 Bot is ready for High-Frequency Trading!")
except ImportError:
    print("❌ Rust module 'polymarket_hft' NOT found.")
    print("   Make sure you ran 'maturin develop --release' in the project root.")
except Exception as e:
    print(f"❌ Error during verification: {e}")
