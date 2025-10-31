# ADVANCED-DATABASE-MANAGEMENT-SYSTEM

# BLOOD BANK & DONOR TRACKING SYSTEM


 ## Blood Bank & Donor Tracking: Distributed Database Management

This repository contains the advanced database management assignments for a two-node (Node_A, Node_B) distributed system,cross-node joins, focusing on horizontal fragmentation, simulating a Blood Bank and Donor Tracking application. The focus is on implementing data fragmentation, distributed transaction control, concurrency diagnosis, declarative integrity,parallel aggregation, two-phase commits, lock diagnostics, and business rules enforcement.
 and advanced data querying   using PostgreSQL concepts (primarily demonstrated via PostgreSQL's Foreign Data Wrapper (FDW and PL/pgSQL).

TECHNOLOGIES USED

• PostgreSQL 15+
• Postgres FDW (Foreign Data Wrapper)
• PL/pgSQL
• SQL Triggers & Functions
• Database Links & Cross-Node Transactions

## SAMPLE DATABASE STRUCTURE

 ### Distributed Database Core Tasks

### Fragment & Recombine Main Fact (Horizontal Fragmentation)

Goal:A Implement horizontal partitioning of the main `Transfusion` fact table across two nodes (`Transfusion_A` on Node_A and `Transfusion_B` on Node_B) and recombine them using a distributed view.

Concept and Implementation Details 

Database Link Simulated via PostgreSQL's 

### CREATE SERVER proj_link FOREIGN DATA WRAPPER postgres_fdw CREATE USER MAPPING

Fragmentation

 Two separate physical tables Transfusion_A Transfusion_B_remote

Recombination

 CREATE VIEW Transfusion_ALL AS ... UNION ALL SELECT * FROM Transfusion_B_remote

Validation

 Use COUNT(*) and a checksum (`SUM(MOD(primary_key, 97))`) to mathematically prove fragmentation consistency

 ### Database Link & Cross-Node Join

Goal: Demonstrate successful access and joining between local data and remote data using the established database link/FDW.

SELECT * FROM blood_unit_remote FETCH FIRST 5 ROWS ONLY;

SELECT T.Transfusion_id, R.full_name, T.Date_Transfused
FROM Transfusion_A T 
JOIN recipient_remote R ON T.Recipient_id = R.recipient_id
WHERE T.StaffID IN (001, 002); 

### Parallel vs Serial Aggregation


Mode           Key Technique                             Execution Plan Feature

Serial     Disable parallel settings 
          (SET max_parallel_workers_per_gather = 0)      Plan shows Gather or Append without explicit parallel workers/slices.

Parallel  Enable and set worker count 
           (SET max_parallel_workers_per_gather = 8)     Plan shows Parallel Append or Parallel Hash Join with multiple Workers.


EXPLAIN (ANALYZE, VERBOSE, BUFFERS)
SELECT
    T.StaffID,
    BU.donor_blood_group,
    COUNT(T.transfusion_id) AS total_transfusions
FROM Transfusion_ALL T
JOIN blood_unit_remote BU ON T.unit_id = BU.unit_id
GROUP BY T.StaffID, BU.donor_blood_group;


### Two-Phase Commit & Recovery (2PC)

Scenario   Local Action                 Remote Action                                   Result 

Success    INSERT INTO Transfusion_A   INSERT INTO blood_unit_remoteBoth     commit successfully (Atomicity).

Failure    INSERT INTO Transfusion_A   INSERT INTO <NON-EXISTENT TABLE>      Remote failure forces a full rollback of the local insert.


DO $$
DECLARE
    new_transfusion_id INT := 600;
    new_unit_id BIGINT := 1000006;
BEGIN
    INSERT INTO blood_unit_remote (unit_id, ...) VALUES (new_unit_id, 'RBC', ...);
    
    INSERT INTO Transfusion_A (transfusion_id, ..., unit_id) VALUES (new_transfusion_id, ..., new_unit_id);
    
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Distributed Transaction FAILED and was rolled back.';
END $$;


DATABASE LINK SETUP (FDW)

CREATE EXTENSION postgres_fdw;

CREATE SERVER BLOOD_BANK_2_server_link
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (host 'localhost', dbname 'BLOOD_BANK_2');

CREATE USER MAPPING FOR postgres
SERVER BLOOD_BANK_2_server_link
OPTIONS (user 'postgres', password 'your_password');


### Distributed Lock Conflict & Diagnosis
 
Session   	         Action	              Result	                             Diagnosis

 
Session 1 (Blocker)	 BEGIN; UPDATE         Holds exclusive lock on             	pg_stat_activity identifies 

                         blood_unit_remote	the remote row (Node B)                 Session 1 as blocking.

Session 2   (Waiter)BEGIN; UPDATE               Waits indefinitely until Session 1       pg_stat_activity identifies Session 2 as 
              blood_unit_remote (same row)       commits/rolls back.                     waiting for a lock (wait_event_type = 'Lock').

SELECT
    waiting.pid AS waiter_pid,
    blocking.pid AS blocker_pid,
    b_locks.relation::regclass AS locked_table
FROM 
    pg_stat_activity waiting
JOIN 
    pg_locks w_locks ON waiting.pid = w_locks.pid AND w_locks.granted = false 
WHERE
    waiting.wait_event_type = 'Lock' 
    AND waiting.pid <> blocking.pid;



### Data Integrity & Advanced Querying Tasks


ALTER TABLE Donation_A
ADD CONSTRAINT CK_VOLUME_RANGE 
CHECK (volume_ML BETWEEN 200 AND 450);

ALTER TABLE blood_unit_remote 
ADD CONSTRAINT CK_EXPIRY_DATE_VALID 
CHECK (expiry_date > collection_date);


#### CORE LOGIC HIGHLIGHTS

Includes unified transfusion view, validation query for checksum comparison, parallel vs serial aggregation examples, and two-phase commit test transaction.

RESULTS SUMMARY

Task	Objective	                 Verified Output
1	Horizontal Fragmentation	✓ COUNT & checksum matched
2	Cross-node Join	                ✓ Remote & local data joined
3	Parallel Aggregation	        ✓ Plans captured via EXPLAIN
4	Two-phase Commit	        ✓ Transaction atomicity proven
5	Lock Conflict	                ✓ Waiter/blocker sessions identified
6–10	Rules, Triggers, Hierarchies	✓ All constraints & logic validated

#### HOW ?

1. Install PostgreSQL 15+
2. Create two databases: BLOOD_BANK_1 and BLOOD_BANK_2
3. Execute the provided SQL scripts in sequence (1_schema.sql, 2_fdw_link.sql, 3_tasks.sql)
4. Verify each task output using validation queries
5. Optional: Run on separate nodes for true distributed simulation


