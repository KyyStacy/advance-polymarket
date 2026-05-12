use alloy_primitives::{Address, U256, keccak256};
use k256::ecdsa::SigningKey;
use rand::RngCore;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

fn parse_hex(s: &str) -> [u8; 32] {
    let s = s.trim_start_matches("0x");
    let bytes = hex::decode(s).unwrap_or_default();
    let mut out = [0u8; 32];
    let len = bytes.len().min(32);
    out[..len].copy_from_slice(&bytes[..len]);
    out
}

pub fn build_and_sign_order(
    chain_id: &str,
    pk: &str,
    app_domain: &str,
    type_hash: &str,
    solady_hash: &str,
    w_name: &str,
    w_version: &str,
    w_salt: &str,
    contents_suffix: &str,
    maker: &str,
    signer: &str,
    token_id: &str,
    maker_amount: &str,
    taker_amount: &str,
    side: u8,
) -> (String, String) {
    let salt = {
        let mut bytes = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut bytes);
        U256::from_be_bytes(bytes)
    };
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    
    let maker_addr = Address::from_str(maker).unwrap_or_default();
    let signer_addr = Address::from_str(signer).unwrap_or_default();
    let t_id = U256::from_str(token_id).unwrap_or_default();
    let m_amt = U256::from_str(maker_amount).unwrap_or_default();
    let t_amt = U256::from_str(taker_amount).unwrap_or_default();
    let chain_id_u256 = U256::from_str(chain_id).unwrap_or_default();

    let type_hash_bytes = parse_hex(type_hash);
    let app_domain_bytes = parse_hex(app_domain);
    let solady_bytes = parse_hex(solady_hash);
    let w_name_bytes = parse_hex(w_name);
    let w_version_bytes = parse_hex(w_version);
    let w_salt_bytes = parse_hex(w_salt);
    
    let signature_type = 3u8; // POLY_1271
    let metadata = [0u8; 32];
    let builder = [0u8; 32]; 

    // 1. Pack Contents Hash
    let mut contents = [0u8; 12 * 32];
    contents[0..32].copy_from_slice(&type_hash_bytes);
    contents[32..64].copy_from_slice(&salt.to_be_bytes::<32>());
    contents[64..96][12..32].copy_from_slice(maker_addr.as_slice());
    contents[96..128][12..32].copy_from_slice(signer_addr.as_slice());
    contents[128..160].copy_from_slice(&t_id.to_be_bytes::<32>());
    contents[160..192].copy_from_slice(&m_amt.to_be_bytes::<32>());
    contents[192..224].copy_from_slice(&t_amt.to_be_bytes::<32>());
    contents[224..256][31] = side;
    contents[256..288][31] = signature_type;
    contents[288..320].copy_from_slice(&U256::from(timestamp).to_be_bytes::<32>());
    contents[320..352].copy_from_slice(&metadata);
    contents[352..384].copy_from_slice(&builder);

    let contents_hash = keccak256(&contents);

    // 2. Pack TypedDataSign Struct Hash
    let mut tds = [0u8; 7 * 32];
    tds[0..32].copy_from_slice(&solady_bytes);
    tds[32..64].copy_from_slice(contents_hash.as_slice());
    tds[64..96].copy_from_slice(&w_name_bytes);
    tds[96..128].copy_from_slice(&w_version_bytes);
    tds[128..160].copy_from_slice(&chain_id_u256.to_be_bytes::<32>());
    tds[160..192][12..32].copy_from_slice(signer_addr.as_slice());
    tds[192..224].copy_from_slice(&w_salt_bytes);

    let tds_hash = keccak256(&tds);

    // 3. Pack Digest
    let mut digest = [0u8; 2 + 32 + 32];
    digest[0] = 0x19;
    digest[1] = 0x01;
    digest[2..34].copy_from_slice(&app_domain_bytes);
    digest[34..66].copy_from_slice(tds_hash.as_slice());

    let final_digest = keccak256(&digest);

    // 4. Sign
    let pk_bytes = hex::decode(pk.trim_start_matches("0x")).unwrap();
    let signing_key = SigningKey::from_slice(&pk_bytes).unwrap();
    let (sig, recid) = signing_key.sign_prehash_recoverable(final_digest.as_slice()).unwrap();
    
    let mut inner_sig = String::with_capacity(130);
    inner_sig.push_str(&hex::encode(sig.to_bytes()));
    inner_sig.push_str(&hex::encode(&[recid.to_byte() + 27]));

    // 5. Construct POLY_1271 Signature
    let final_signature = format!("0x{}{}{}{}", inner_sig, hex::encode(app_domain_bytes), hex::encode(contents_hash.as_slice()), contents_suffix);

    // 6. Construct JSON
    let side_str = if side == 0 { "BUY" } else { "SELL" };
    let order_json = format!(r#"{{"salt":{},"maker":"{}","signer":"{}","tokenId":"{}","makerAmount":"{}","takerAmount":"{}","side":"{}","signatureType":{},"timestamp":"{}","metadata":"0x0000000000000000000000000000000000000000000000000000000000000000","builder":"0x0000000000000000000000000000000000000000000000000000000000000000","expiration":"0","signature":"{}"}}"#,
        salt, maker_addr.to_string(), signer_addr.to_string(), t_id, m_amt, t_amt, side_str, signature_type, timestamp, final_signature
    );

    (order_json, final_signature)
}
