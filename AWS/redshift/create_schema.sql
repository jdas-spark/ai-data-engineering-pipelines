CREATE TABLE IF NOT EXISTS public.transaction_scores (
    transaction_id VARCHAR(50),
    user_id VARCHAR(50),
    amount FLOAT,
    merchant VARCHAR(100),
    fraud_score FLOAT
);