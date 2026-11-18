-- Heartbeat table for the Proxy's application-level replication-lag probe.
-- The Proxy updates row 1 on the primary and times how long until each
-- replica sees the new seq.
CREATE DATABASE IF NOT EXISTS proxy_meta;

CREATE TABLE IF NOT EXISTS proxy_meta.heartbeat (
  id         TINYINT      NOT NULL PRIMARY KEY,
  seq        BIGINT       NOT NULL,
  written_at TIMESTAMP(6) NOT NULL
);

INSERT INTO proxy_meta.heartbeat (id, seq, written_at) VALUES (1, 0, NOW(6))
  ON DUPLICATE KEY UPDATE id = id;
