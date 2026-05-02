from __future__ import annotations

import asyncio

from web3 import Web3

from ..utils.logger import get_logger

log = get_logger()

# Polygon addresses (mainnet)
CTF_CONTRACT = Web3.to_checksum_address("0x4D97DCd97eC945f40cF65F87097ACe5EA0476045")
USDC_POLYGON = Web3.to_checksum_address("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174")

CTF_ABI = [
    {
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "indexSets", "type": "uint256[]"},
        ],
        "name": "redeemPositions",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "conditionId", "type": "bytes32"},
        ],
        "name": "payoutNumerators",
        "outputs": [{"name": "", "type": "uint256[]"}],
        "stateMutability": "view",
        "type": "function",
    },
]


class PositionRedeemer:
    """Redeems winning Polymarket positions via on-chain CTF contract.

    Called after each 5-minute window when AUTO_CLAIM=true and we held a
    position through to resolution.

    Up   = outcome index 0 → index_set = 1  (binary: 01)
    Down = outcome index 1 → index_set = 2  (binary: 10)
    """

    def __init__(self, rpc_url: str, private_key: str, gas_limit: int = 150_000):
        first_rpc = rpc_url.split(",")[0].strip()
        self._w3 = Web3(Web3.HTTPProvider(first_rpc, request_kwargs={"timeout": 10}))
        self._pk = private_key
        self._account = self._w3.eth.account.from_key(private_key) if private_key else None
        self._ctf = self._w3.eth.contract(address=CTF_CONTRACT, abi=CTF_ABI)
        self._gas_limit = gas_limit

    def _condition_id_bytes(self, condition_id_hex: str) -> bytes:
        h = condition_id_hex.replace("0x", "").replace("0X", "")
        return bytes.fromhex(h.zfill(64))

    def _is_resolved(self, condition_id_hex: str) -> bool:
        try:
            nums = self._ctf.functions.payoutNumerators(
                self._condition_id_bytes(condition_id_hex)
            ).call()
            return len(nums) > 0 and sum(nums) > 0
        except Exception:
            return False

    async def redeem(
        self,
        condition_id: str,
        side: str,
        up_outcome_idx: int = 0,
    ) -> bool:
        """Attempt to redeem positions for the given condition after resolution.

        Returns True if tx was submitted (regardless of outcome).
        """
        if not self._account:
            log.warning("[redeemer] no private key configured — skip redeem")
            return False

        resolved = await asyncio.to_thread(self._is_resolved, condition_id)
        if not resolved:
            log.debug(f"[redeemer] condition {condition_id[:10]}… not resolved yet, skip")
            return False

        # index_set: Up=1 (outcome 0, bit 0), Down=2 (outcome 1, bit 1)
        if side == "UP":
            index_sets = [1 << up_outcome_idx]
        else:
            index_sets = [1 << (1 - up_outcome_idx)]

        try:
            nonce = await asyncio.to_thread(
                self._w3.eth.get_transaction_count, self._account.address
            )
            gas_price = await asyncio.to_thread(self._w3.eth.gas_price)
            tx = self._ctf.functions.redeemPositions(
                USDC_POLYGON,
                b"\x00" * 32,
                self._condition_id_bytes(condition_id),
                index_sets,
            ).build_transaction(
                {
                    "from": self._account.address,
                    "nonce": nonce,
                    "gas": self._gas_limit,
                    "gasPrice": int(gas_price * 1.15),
                }
            )
            signed = self._account.sign_transaction(tx)
            tx_hash = await asyncio.to_thread(
                self._w3.eth.send_raw_transaction, signed.raw_transaction
            )
            log.info(
                f"[redeemer] redeem tx {tx_hash.hex()[:12]}… "
                f"condition={condition_id[:10]}… side={side}"
            )
            return True
        except Exception as e:
            log.warning(f"[redeemer] redeem failed: {e}")
            return False
