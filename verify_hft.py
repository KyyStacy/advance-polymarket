import sys
import time

try:
    import polymarket_hft
    print("✅ Rust module 'polymarket_hft' found and loaded successfully!")
    
    # Simple check of classes
    creds = polymarket_hft.RustApiCreds("test_key", "test_secret", "test_pass")
    pool = polymarket_hft.RustHttpPool("https://clob.polymarket.com", 1100)
    
    print(f"✅ Class instantiation works: {creds}")
    print(f"✅ Pool initialization works: {pool}")
    
    print("\n🚀 Bot is ready for High-Frequency Trading!")
except ImportError:
    print("❌ Rust module 'polymarket_hft' NOT found.")
    print("   Make sure you ran 'maturin develop --release' in the project root.")
except Exception as e:
    print(f"❌ Error during verification: {e}")
