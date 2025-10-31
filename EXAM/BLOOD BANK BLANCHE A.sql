CREATE SCHEMA BranchDB_A;

CREATE TABLE Donor (
    DonorID INT PRIMARY KEY,
    FullName VARCHAR(255),
    Gender VARCHAR(10),
    BloodGroup VARCHAR(5),
    Contact VARCHAR(20),
    City VARCHAR(100)
);

INSERT INTO Donor(DonorID, FullName, Gender, BloodGroup, Contact, City) VALUES
('101', 'Carl Smith', 'male', 'A', '555_1011', 'KIGALI'),
('102', 'Linda Green', 'female', 'O', '555_1012', 'KIGALI'),
('103', 'John Doe', 'male', 'B', '555_1013', 'KIGALI'),
('104', 'Amy Wong', 'female', 'AB', '555_1014', 'KIGALI'),
('105', 'Kevin Hill', 'male', 'O', '555_1015', 'KIGALI');

SELECT * FROM Donor;
DROP TABLE  Staff CASCADE;
CREATE TABLE Staff (
    StaffID INT PRIMARY KEY,
    FullName VARCHAR(255),
    Role VARCHAR(100),
    Department VARCHAR(100),
    Contact VARCHAR(20)
);

INSERT INTO Staff (StaffID, FullName, Role, Department, Contact) VALUES
('001', 'Alice Johnson', 'Phlebotomist', 'Collection', '555-0101'),
('002', 'Robert Lee', 'Lab Technician', 'Testing', '555-0102'),
('003', 'Maria Garcia', 'Nurse', 'Collection', '555-0103'),
('004', 'David Kim', 'Inventory Manager', 'Storage', '555-0104'),
('005', 'Sarah Chen', 'Transfusion Specialist', 'Distribution', '555-0105');


CREATE TABLE donation_A(
donation_id INT PRIMARY KEY,
donorID INT  NOT NULL REFERENCES donor(donorID),
date_donated DATE NOT NULL DEFAULT CURRENT_DATE,
volume_ML INT NOT NULL CHECK (volume_ml BETWEEN 200 AND 450),
tested_status VARCHAR (10) NOT NULL, 
blood_type VARCHAR(20 ) NOT NULL 
);
DROP TABLE Transfusion_A CASCADE;
CREATE TABLE Transfusion_A (
    Transfusion_id INT PRIMARY KEY,
    Unit_id INT NOT NULL, -- FK to BloodUnit_A (local) or BloodUnit_B (remote)
    Recipient_id INT NOT NULL,
	StaffID INT NOT NULL,
    Date_Transfused DATE NOT NULL,
    CONSTRAINT FK_TRANSFA_UNIT FOREIGN KEY (Unit_id) REFERENCES blood_unit_remote(Unit_id),
	CONSTRAINT FK_TRANSFA_Staff FOREIGN KEY (StaffID) REFERENCES Staff(StaffID),
    CONSTRAINT FK_TRANSFA_RECIPIENT FOREIGN KEY (recipient_id) REFERENCES recipient_remote(recipient_id)
	);
SELECT*FROM Transfusion_A;
INSERT INTO Transfusion_A (transfusion_id,StaffID, recipient_id,Unit_id, date_transfused) 
VALUES 
('1',001,100, 1000001,'2025-10-20'), 
('2',002,200, 1000002,'2025-10-21'), 
('3',003,300, 1000003,'2025-10-22'), 
('4',004,400, 1000004,'2025-10-23'), 
('5',005,500, 1000005,'2025-10-24');
COMMIT;
 SELECT * FROM Transfusion_A;
DROP TABLE transfusion_B_remote CASCADE;
CREATE TABLE transfusion_B_remote(
    TransfusionB_id INT PRIMARY KEY,
    Unit_id INT NOT NULL, -- FK to BloodUnit_A (local) or BloodUnit_B (remote)
    Recipient_id INT NOT NULL,
	StaffID INT NOT NULL,
    Date_Transfused DATE NOT NULL,
    CONSTRAINT FK_TRANSFA_UNIT FOREIGN KEY (Unit_id) REFERENCES blood_unit_remote(Unit_id),
	CONSTRAINT FK_TRANSFA_Staff FOREIGN KEY (StaffID) REFERENCES Staff(StaffID),
    CONSTRAINT FK_TRANSFA_RECIPIENT FOREIGN KEY (recipient_id) REFERENCES recipient_remote(recipient_id)
	);

INSERT INTO Transfusion_B_remote(transfusionB_id,StaffID, recipient_id, date_transfused, unit_id) 
VALUES 
(5001,001, 10,'2025-10-25',1000001), 
(5002,002, 20,'2025-10-26',1000002), 
(5003,003, 30,'2025-10-27',1000003), 
(5004,004, 40,'2025-10-28',1000004), 
(5005,005, 50,'2025-10-29',1000005); 
COMMIT;
SELECT * FROM Transfusion_B_remote ;

CREATE TABLE recipient_remote(
recipient_id INT PRIMARY KEY,
full_name VARCHAR(20) NOT NULL,
blood_group VARCHAR(5) NOT NULL,
hospital VARCHAR(20) NOT NULL,
contact VARCHAR(20) NOT NULL
);
INSERT INTO recipient_remote (recipient_id, full_name, blood_group, hospital, contact)
VALUES 
    (100, 'OSCAR', 'A+', 'Hospital X', '555_1010'),
    (200, 'ERIC', 'B+', 'Hospital Y', '555_2020'),
    (300, 'EMMANUEL', 'O-', 'Hospital Z', '555_3030'),
    (400, 'EVA', 'AB-', 'Hospital X', '555_4040'),
    (500, 'EDA', 'A-', 'Hospital Y', '555_5050');

SELECT * FROM recipient_remote;

COMMIT;
DROP TABLE blood_unit_remote CASCADE;
CREATE TABLE blood_unit_remote(
unit_id INT PRIMARY KEY,
unit_type VARCHAR(20)NOT NULL,
donor_blood_group VARCHAR(3) NOT NULL,
collection_date DATE NOT NULL,
expiry_date DATE NOT NULL,
availability BOOLEAN NOT NULL
);

SELECT * FROM blood_unit_remote 
;
CREATE ROLE your_local_user WITH LOGIN PASSWORD 'Nj0783708910@';

CREATE SCHEMA public;

CREATE EXTENSION postgres_fdw;

CREATE SERVER BLOOD_BANK_2_server_link
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (host 'localhost', dbname 'BLOOD BANK 2'); 

CREATE USER MAPPING FOR postgres
SERVER BLOOD_BANK_2_server_link
OPTIONS (user 'postgres', password 'Nj0783708910@');

-- 5. Recombination View (A1)
CREATE VIEW transfusion_all AS 
    SELECT * FROM transfusion_A
    UNION ALL
    SELECT * FROM transfusion_B_remote;

	

WITH FragmentMetrics AS (
    -- 1. Metrics from Local Fragment A
    SELECT 
        COUNT(*)::BIGINT AS count_a, 
        SUM(MOD(transfusion_id, 97)) AS checksum_a 
    FROM Transfusion_A
),
RemoteMetrics AS (
    -- 2. Metrics from Remote Fragment B (via FDW)
    SELECT 
        COUNT(*)::BIGINT AS count_b, 
        SUM(MOD(transfusionB_id, 97)) AS checksum_b 
    FROM transfusion_B_remote
)
-- Combine and compare
SELECT
    -- A. Combined Fragments Metrics
    fm.count_a + rm.count_b AS total_fragments_count,
    fm.checksum_a + rm.checksum_b AS total_fragments_checksum,
    
    -- B. View Metrics
    (SELECT COUNT(*) FROM Transfusion_ALL) AS view_total_count,
    (SELECT SUM(MOD(transfusion_id, 97)) FROM Transfusion_ALL) AS view_total_checksum,
    
    -- C. Validation Check
    CASE WHEN 
        (fm.count_a + rm.count_b) = (SELECT COUNT(*) FROM Transfusion_ALL) AND
        (fm.checksum_a + rm.checksum_b) = (SELECT SUM(MOD(transfusion_id, 97)) FROM Transfusion_ALL)
    THEN '✓ MATCHED'
    ELSE '✗ FAILED' END AS validation_result
FROM 
    FragmentMetrics fm, 
    RemoteMetrics rm;	





-- 1. Define the Foreign Server (Equivalent of CREATE DATABASE LINK 'proj_link')
-- This creates the server link 'proj_link' that connects Blanche A (Node_A) 
-- to the remote database 'BLOOD BANK 2' (Blanche B).
-- ASSUMPTION: 'localhost' and 'BLOOD BANK 2' are the correct connection details
-- for the remote server instance.
CREATE SERVER proj_link
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (host 'localhost', dbname 'BLOOD BANK 2'); 

-- 2. Create the User Mapping (Authentication)
-- This defines the credentials used by the local user ('postgres' in this case) 
-- when connecting to the remote server 'proj_link'.
-- It allows Node A to authenticate against Node B.
CREATE USER MAPPING FOR postgres
SERVER proj_link
OPTIONS (user 'postgres', password 'Nj0783708910@');


INSERT INTO blood_unit_remote (unit_id, unit_type, donor_blood_group, collection_date, expiry_date, availability)
VALUES 
    (1000001, 'Whole', 'O+', '2025-09-01', '2025-10-31', TRUE),
    (1000002, 'RBC', 'A-', '2025-09-02', '2025-11-01', TRUE),
    (1000003, 'Plasma', 'B+', '2025-09-03', '2025-11-02', TRUE),
    (1000004, 'Platelets', 'AB+', '2025-09-04', '2025-11-03', TRUE),
    (1000005, 'Whole', 'O-', '2025-09-05', '2025-11-04', TRUE);
COMMIT;

-- RUN THIS ON BLANCHE A to confirm data access:
SELECT * FROM blood_unit_remote FETCH FIRST 5 ROWS ONLY;



-- A3.1: SERIAL Aggregation on Transfusion_ALL

-- 1. Configuration: Disable Parallel Query Features to force a SERIAL plan
-- This is crucial for demonstrating the difference between serial and parallel processing.
SET max_parallel_workers_per_gather = 0;
SET enable_parallel_append = off;
SET enable_parallel_hash = off;

-- 2. SERIAL Aggregation Query
-- Goal: Calculate the number of transfusions handled by each staff member, 
-- grouped by the staff member AND the blood group of the unit they transfused.
-- The result will have 10 rows (2 staff * 5 unit types), fulfilling the 3-10 groups requirement.

SELECT
    T.StaffID,
    BU.donor_blood_group,
    COUNT(T.transfusion_id) AS total_transfusions,
    MIN(T.date_transfused) AS earliest_date
FROM
    Transfusion_ALL T -- Unified view (Local A + Remote B data)
JOIN
    blood_unit_remote BU ON T.unit_id = BU.unit_id -- Remote join to Blanche B
GROUP BY 
    T.StaffID, 
    BU.donor_blood_group
ORDER BY 
    T.StaffID, 
    BU.donor_blood_group;
SET max_parallel_workers_per_gather = 8;
SET max_parallel_workers = 8;
SELECT
    T.StaffID,
    BU.donor_blood_group,
    COUNT(T.transfusion_id) AS total_transfusions,
    MIN(T.date_transfused) AS earliest_date
FROM
    Transfusion_ALL T
JOIN
    blood_unit_remote BU ON T.unit_id = BU.unit_id
GROUP BY
    T.StaffID,
    BU.donor_blood_group
ORDER BY
    T.StaffID,
    BU.donor_blood_group;

	
RESET max_parallel_workers_per_gather;
RESET max_parallel_workers;


-- 1. Configuration: Enable Parallel Query Features and set max workers to 8
SET max_parallel_workers_per_gather = 8;
SET max_parallel_workers = 8;




--  Run the aggregation query with EXPLAIN ANALYZE to capture execution plans and statistics.
EXPLAIN (ANALYZE, VERBOSE, BUFFERS)
SELECT
    T.StaffID,
    BU.donor_blood_group,
    COUNT(T.transfusion_id) AS total_transfusions,
    MIN(T.date_transfused) AS earliest_date
FROM
    Transfusion_ALL T
JOIN
    blood_unit_remote BU ON T.unit_id = BU.unit_id
GROUP BY
    T.StaffID,
    BU.donor_blood_group
ORDER BY
    T.StaffID,
    BU.donor_blood_group;

-- 3. Reset settings to their default values (recommended)
RESET max_parallel_workers_per_gather;
RESET max_parallel_workers;


-- Disable parallel features for the session
SET max_parallel_workers_per_gather = 0;
SET enable_parallel_append = off;
SET enable_parallel_hash = off;

-- Run the query with EXPLAIN ANALYZE to get the serial plan and stats
EXPLAIN (ANALYZE, VERBOSE, BUFFERS)
SELECT
    T.StaffID,
    BU.donor_blood_group,
    COUNT(T.transfusion_id) AS total_transfusions,
    MIN(T.date_transfused) AS earliest_date
FROM
    Transfusion_ALL T
JOIN
    blood_unit_remote BU ON T.unit_id = BU.unit_id
GROUP BY
    T.StaffID,
    BU.donor_blood_group
ORDER BY
    T.StaffID,
    BU.donor_blood_group;

-- Reset settings to default
RESET max_parallel_workers_per_gather;
RESET enable_parallel_append;
RESET enable_parallel_hash;


-- Set parallel features for the session
SET max_parallel_workers_per_gather = 8;
SET max_parallel_workers = 8;

-- Run the query with EXPLAIN ANALYZE to get the parallel plan and stats
EXPLAIN (ANALYZE, VERBOSE, BUFFERS)
SELECT
    T.StaffID,
    BU.donor_blood_group,
    COUNT(T.transfusion_id) AS total_transfusions,
    MIN(T.date_transfused) AS earliest_date
FROM
    Transfusion_ALL T
JOIN
    blood_unit_remote BU ON T.unit_id = BU.unit_id
GROUP BY
    T.StaffID,
    BU.donor_blood_group
ORDER BY
    T.StaffID,
    BU.donor_blood_group;

-- Reset settings to default
RESET max_parallel_workers_per_gather;
RESET max_parallel_workers;

rollback;
-- A4: DISTRIBUTED TRANSACTION (PL/pgSQL DO Block)
-- This block ensures Atomicity: both local and remote inserts will commit or rollback together.
-- It creates a new Transfusion record (Local) and the corresponding BloodUnit (Remote).
DO $$
DECLARE
    new_transfusion_id INT := 600;
    new_unit_id BIGINT := 1000006;
    -- Note: Local date_transfused must be later than Remote collection_date
    transfusion_dt DATE := '2025-10-29';
    collection_dt DATE := '2025-10-28';
BEGIN
    -- 1. REMOTE INSERT: Insert a new BloodUnit record into the remote table on Node B.
    -- This uses the FDW link ('proj_link') underlying 'blood_unit_remote'.
    INSERT INTO blood_unit_remote (
        unit_id, unit_type, donor_blood_group, collection_date, expiry_date, availability
    )
    VALUES (
        new_unit_id, 'RBC', 'AB-', collection_dt, '2025-12-28', TRUE
    );
 rollback;
    -- 2. LOCAL INSERT: Insert a new Transfusion record into the local fragment on Node A.
    -- This record references the BloodUnit created in the step above.
    INSERT INTO Transfusion_A (
        transfusion_id, recipient_id, date_transfused, StaffID, unit_id
    )
    VALUES (
        new_transfusion_id, 30, transfusion_dt, 1, new_unit_id
    );
rollback;
    -- 3. COMMIT: The transaction is implicitly committed successfully at the end of the DO block.
    RAISE NOTICE 'Distributed Transaction Successful: Transfusion ID % inserted locally, Unit ID % inserted remotely.', new_transfusion_id, new_unit_id;

EXCEPTION
    WHEN OTHERS THEN
        -- If any error occurs (local or remote), the transaction is implicitly rolled back.
        RAISE NOTICE 'Distributed Transaction FAILED and was rolled back: %', SQLERRM;
END $$;
rollback;

-- Confirm the new row is visible in the unified view:
SELECT COUNT(*) FROM Transfusion_ALL; -- Expected result: 11
 rollback;

-- A4 - Second Run: INDUCING DISTRIBUTED FAILURE & ROLLBACK
-- This transaction will attempt to insert a local row, but the subsequent 
-- remote insert will intentionally fail (using a non-existent table), 
-- forcing a full transaction rollback.
DO $$
DECLARE
    failing_transfusion_id INT := 601;
    failing_unit_id BIGINT := 1000007;
    transfusion_dt DATE := '2025-10-30';
BEGIN
    -- 1. LOCAL INSERT: Insert a new Transfusion record into the local fragment on Node A.
    -- This operation succeeds initially.
    INSERT INTO Transfusion_A (
        transfusion_id, recipient_id, date_transfused, StaffID, unit_id
    )
    VALUES (
        failing_transfusion_id, 30, transfusion_dt, 1, failing_unit_id
    );

    RAISE NOTICE 'Local insert SUCCESSFUL (Transfusion ID %)... attempting remote operation.', failing_transfusion_id;

    -- 2. REMOTE INSERT (FAILURE POINT): Attempt to insert into a non-existent foreign table.
    -- This simulates the remote link/service being broken or the database crashing.
    -- NOTE: This table (blood_unit_FAIL_remote) must NOT exist on Blanche A or Blanche B.
    INSERT INTO blood_unit_FAIL_remote (
        unit_id, unit_type, donor_blood_group, collection_date, expiry_date, availability
    )
    VALUES (
        failing_unit_id, 'RBC', 'AB-', '2025-10-29', '2025-12-29', TRUE
    );
rollback;
    -- 3. COMMIT: This line is never reached.
    
EXCEPTION
    WHEN OTHERS THEN
        -- The EXCEPTION block catches the remote failure and reports the full rollback.
        -- This demonstrates the atomicity of the distributed transaction.
        RAISE NOTICE '!!! Distributed Transaction FAILURE !!!';
        RAISE NOTICE 'Error details: %', SQLERRM;
        RAISE NOTICE 'The local insert (Transfusion ID %) was implicitly ROLLED BACK.', failing_transfusion_id;
        -- The transaction ends here, guaranteeing no committed rows.
END $$;

rollback;
-- Confirm the failed row was rolled back:
SELECT COUNT(*) FROM Transfusion_ALL WHERE transfusion_id = 601; 
-- Expected result: 0

SELECT COUNT(*) FROM Transfusion_ALL;
-- Expected result: 10 (or 11 if previous ID 600 was not manually deleted)


-- Querying for active locks related to the foreign server (Closest equivalent to checking active distributed transactions)
SELECT 
    locktype, 
    mode, 
    granted, 
    pid, 
    coalesce(relname, 'non-relation lock') AS object_name
FROM 
    pg_locks pl
LEFT JOIN 
    pg_class pc ON pl.relation = pc.oid
WHERE 
    pid <> pg_backend_pid() AND granted = TRUE; -- Show locks held by other sessions

-- Querying for truly prepared 2PC transactions (Usually empty unless explicitly used)
SELECT * FROM pg_prepared_xacts;
-- Expected Result: 0 rows (confirms no in-doubt state was left)

rollback;
--  Final Cleanup: ROLLBACK FORCE equivalent (Manual Deletion)
-- This action ensures the total committed row count remains <= 10 as required by the exam.

-- 1. Delete the local Transfusion row (ID 600)
DELETE FROM Transfusion_A WHERE transfusion_id = 600;

-- 2. Delete the remote BloodUnit row (ID 1000006)
DELETE FROM blood_unit_remote WHERE unit_id = 1000006;

-- 3. Commit the cleanup
COMMIT;

-- Re-verify Consistency on Node A
SELECT COUNT(*) AS final_total_committed_rows FROM Transfusion_ALL;
-- Expected Result: 10 (The 5 initial rows from A + 5 initial rows from B)

rollback;

--  Final Verification: Clean Run Confirmation

-- Query the PostgreSQL system catalog for any active two-phase commit (2PC) transactions.
-- This confirms the system is clean and no transaction is stuck "in-doubt."
SELECT 
    gid, 
    prepared AS prepared_at, -- Use 'prepared' and alias it for clarity
    owner, 
    database 
FROM 
    pg_prepared_xacts;

rollback;
 -- A5: DISTRIBUTED CONCURRENCY CONTROL SETUP (SESSION 1)

-- 1. Configuration: Ensure Autocommit is OFF (crucial for holding the lock)
-- In many clients, this is SET autocommit = OFF or a client configuration.
-- If your client uses explicit transaction blocks, BEGIN is sufficient.
BEGIN;

-- 2. UPDATE Statement on REMOTE table (BloodUnit@proj_link)
-- This acquires and holds an exclusive row lock on unit_id = 1000001 on the remote database (Node B).
UPDATE blood_unit_remote 
SET availability = FALSE 
WHERE unit_id = 1000001; 

-- IMPORTANT: DO NOT RUN COMMIT OR ROLLBACK. 
-- KEEP THIS SESSION 1 WINDOW OPEN AND WAITING.
-- Proceed to the next step (Session 2) immediately.


-- A5: QUERYING POSTGRESQL LOCK VIEWS (Equivalent to DBA_BLOCKERS/DBA_WAITERS)
-- This query identifies the session holding the lock (BLOCKER) and the session waiting (WAITER).
SELECT
    -- WAITER Information
    waiting.pid AS waiter_pid,
    waiting.usename AS waiter_user,
    waiting.client_addr AS waiter_client,
    waiting.query AS waiter_query,
    
    -- BLOCKER Information
    blocking.pid AS blocker_pid,
    blocking.usename AS blocker_user,
    blocking.client_addr AS blocker_client,
    blocking.query AS blocker_query,
    
    -- Lock Information
    b_locks.locktype,
    b_locks.relation::regclass AS locked_table,
    b_locks.mode AS blocker_lock_mode,
    w_locks.mode AS waiter_lock_mode
FROM 
    pg_stat_activity waiting
JOIN 
    pg_locks w_locks ON waiting.pid = w_locks.pid AND w_locks.granted = false -- Session waiting for lock
JOIN 
    pg_locks b_locks ON b_locks.locktype = w_locks.locktype 
                      AND b_locks.database = w_locks.database
                      AND b_locks.relation = w_locks.relation
                      AND b_locks.granted = true -- Session holding the lock
JOIN 
    pg_stat_activity blocking ON blocking.pid = b_locks.pid
WHERE
    waiting.wait_event_type = 'Lock' -- Focus only on sessions waiting for a lock
    AND waiting.pid <> blocking.pid; -- Exclude self-blocking
rollback;
COMMIT;

-- A5: RELEASE LOCK
-- Running COMMIT releases the exclusive row lock held on Node B.
COMMIT;
-- Session 1 successfully commits its update (availability = FALSE) and finishes.

-- A5: FINAL VALIDATION
-- Query the remote table via FDW to confirm the final state set by Session 2.
SELECT 
    unit_id, 
    availability 
FROM 
    blood_unit_remote 
WHERE 
    unit_id = 1000001;

rollback;

CREATE TABLE Donation_A_UDIT (
    bef_total NUMERIC(12, 2),
    aft_total NUMERIC(12, 2),
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    key_col VARCHAR(64)
);


rollback;

CREATE OR REPLACE FUNCTION trg_recalculate_donation_totals_stmt()
RETURNS TRIGGER AS $$
DECLARE
    -- Collect all unique Donation IDs that have been affected by the statement
    affected_donation_ids INTEGER[];
    donation_id_val INTEGER;
BEGIN
    -- Determine which donation IDs were affected based on the operation type.
    IF TG_OP = 'INSERT' THEN
        -- Collect donation IDs from newly inserted rows
        SELECT array_agg(DISTINCT donationID) INTO affected_donation_ids FROM new_table;
    ELSIF TG_OP = 'UPDATE' THEN
        -- Collect donation IDs from both old and new rows
        SELECT array_agg(DISTINCT id) INTO affected_donation_ids
        FROM (
            SELECT donation_id AS id FROM new_table
            UNION
            SELECT donation_id AS id FROM old_table
        ) AS affected_ids;
    ELSIF TG_OP = 'DELETE' THEN
        -- Collect donation IDs from deleted rows
        SELECT array_agg(DISTINCT donation_id) INTO affected_donation_ids FROM old_table;
    END IF;

    -- Update totals for all affected donations
    FOREACH donation_id_val IN ARRAY affected_donation_ids LOOP
        UPDATE Donation d
        SET total = (
            SELECT COALESCE(SUM(bu.volume_ML), 0)
            FROM BloodUnit bu
            WHERE bu.donation_id = d.donation_id
        )
        WHERE d.donation_id = donation_id_val;
    END LOOP;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

rollback;
-- Trigger for INSERT events
CREATE TRIGGER trg_recalculate_donation_totals_insert
AFTER INSERT ON blood_unit_remote 
REFERENCING NEW TABLE AS new_table
FOR EACH STATEMENT
EXECUTE FUNCTION trg_recalculate_donation_totals_stmt();

-- Trigger for UPDATE events
CREATE TRIGGER trg_recalculate_donation_totals_update
AFTER UPDATE ON blood_unit_remote 
REFERENCING OLD TABLE AS old_table NEW TABLE AS new_table
FOR EACH STATEMENT
EXECUTE FUNCTION trg_recalculate_donation_totals_stmt();

-- Trigger for DELETE events
CREATE TRIGGER trg_recalculate_donation_totals_delete
AFTER DELETE ON blood_unit_remote 
REFERENCING OLD TABLE AS old_table
FOR EACH STATEMENT
EXECUTE FUNCTION trg_recalculate_donation_totals_stmt();

rollback;
ALTER TABLE Donation_A ADD COLUMN total NUMERIC(12, 2) DEFAULT 0;




rollback;




BEGIN;

-- Insert a new row (1 row affected)
INSERT INTO blood_Unit_remote (unit_id, unit_type, donor_blood_group, collection_date, expiry_date, availability)
VALUES (1003, 'Plasma', 'B+', CURRENT_DATE, '2026-03-01', TRUE);

-- Update an existing row (1 row affected)
UPDATE blood_Unit_remote
SET availability = FALSE
WHERE unit_id = 1001;

-- Delete a row (1 row affected)
DELETE FROM blood_Unit_remote
WHERE unit_id = 1002;

-- Check the affected row count (optional)
-- You can manually count the changes to ensure you stay within the limit.
-- In this case, 1 insert, 1 update, and 1 delete, for a total of 3 affected rows.

COMMIT;




-- A6.2: PASSING INSERTS AND CLEANUP (Demonstrates success and restores budget)

-- 1. Insert 4 valid rows (2 Donation, 2 BloodUnit)
INSERT INTO Donation (donation_id, DonorID, date_donated, volume_ML)
VALUES 
    (7, 30, CURRENT_DATE, 450), -- PASS 1
    (8, 40, CURRENT_DATE, 475); -- PASS 2

INSERT INTO blood_Unit_remote (unit_id, unit_type, donor_blood_group, collection_date, expiry_date, availability, donation_id)
VALUES 
    (1000007, 'Whole', 'A-', '2025-10-05', '2025-12-05', TRUE, 7), -- PASS 3
    (1000008, 'Plasma', 'B+', '2025-10-06', '2025-11-06', TRUE, 8); -- PASS 4

COMMIT;

RAISE NOTICE '4 validation rows successfully committed.';

-- 2. CLEANUP: Delete the 4 rows immediately to maintain the row budget.
DELETE FROM blood_Unit WHERE unit_id IN (1000007, 1000008);
DELETE FROM Donation WHERE donation_id IN (7, 8);

COMMIT;
RAISE NOTICE 'Cleanup complete. Committed row count is restored to <= 10.';




-- A7: CREATE AUDIT TABLE
-- This table tracks the total number of donations before and after a change, 
-- demonstrating data integrity across the distributed system.

CREATE TABLE Donation_AUDIT (
    bef_total NUMERIC,          -- PostgreSQL equivalent of Oracle NUMBER: Total row count BEFORE the change
    aft_total NUMERIC,          -- Total row count AFTER the change
    changed_at TIMESTAMP,       -- PostgreSQL TIMESTAMP for tracking WHEN the change occurred
    key_col VARCHAR(64)         -- Tracking column to store information about the change (e.g., 'INSERT', 'DELETE')
);

-- Note: We use NUMERIC here to accommodate any large count, though BIGINT would also work.




	