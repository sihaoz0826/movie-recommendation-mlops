CREATE TABLE routing_decisions (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    user_id INT NOT NULL,
    inference_instance NVARCHAR(50) NOT NULL,  -- 'inference-1' or 'inference-2'
    timestamp DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    response_status INT,
    response_text NVARCHAR(200),  -- Stores the comma-separated movie IDs response
    INDEX IX_routing_user_time (user_id, timestamp)
);

