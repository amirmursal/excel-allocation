# Allocation Preference Rules and Combinations

This document outlines all allocation preference combinations and their allocation rules/orders.

## Overview

Allocation preferences determine how work is allocated to agents based on their capabilities and preferences. The system checks the "Insurance List" column to verify agent capabilities before allocating work.

---

## Basic Allocation Preferences

### 1. "PB"

**Allocation Rule:**

- Allocates "NTBP" remark rows to agents with PB preference
- Distribution:
  - If NTBP count ≥ total agent capacity: Distribute equally (round-robin)
  - If NTBP count < total agent capacity: Allocate to single agent (highest capacity first)

**Step:** Step 2.5 (Global NTBP Allocation)

---

### 2. "NTC"

**Allocation Rule:**

- Allocates "NTC" remark rows to agents with NTC preference
- Distribution:
  - If NTC count ≥ combined agent capacity: Distribute equally (round-robin)
  - If NTC count < combined agent capacity: Allocate to single agent (highest capacity first)

**Step:** Step 3.5 (Global NTC Allocation)

---

### 3. "Single"

**Allocation Rule:**

- Allocates same insurance company rows to the agent
- Logic:
  1. Find first row where "Dental Primary Ins Carr" matches agent's "Insurance List" capabilities
  2. Assign that insurance company to the agent
  3. Continue allocating rows with the same insurance company until capacity is full
- Checks "Insurance List" column for capabilities before allocation

**Step:** Step 3.7 (Global Single Allocation)

---

### 4. "Mix"

**Allocation Rule:**

- Allocates multiple insurance company rows to the agent
- Logic:
  1. Find unallocated rows from multiple insurance companies
  2. Check if insurance from "Dental Primary Ins Carr" is in agent's "Insurance List" capabilities
  3. Allocate rows until capacity is full
- Skips: Secondary insurance rows, NTBP rows, NTC rows

**Step:** Step 3.8 (Global Mix Allocation)

---

## Combined Allocation Preferences

### 5. "Mix + NTC"

**Allocation Rule:**

- **Phase 1:** Allocate NTC remark rows first
- **Phase 2:** If capacity remains, allocate other insurance company rows (Mix logic)
- Logic:
  - Phase 1: NTC rows are allocated in Step 3.5 (Global NTC Allocation)
  - Phase 2: After NTC allocation, if capacity remains, allocate multiple insurance company rows
  - Checks "Insurance List" column for capabilities before allocation
- Skips: Secondary insurance rows, NTBP rows

**Steps:**

- Step 3.5 (NTC Allocation)
- Step 3.5.5 (Mix + NTC - Other Insurance Rows)

---

### 6. "Sec + Single"

**Allocation Rule:**

- **Phase 1:** Allocate secondary insurance rows first (from "Dental Secondary Ins Carr" column)
- **Phase 2:** If capacity remains, allocate same insurance company rows (from "Dental Primary Ins Carr")
- Logic:
  - Phase 1: Secondary insurance rows are allocated in Step 3.6 (Global Secondary Insurance Allocation)
  - Phase 2: After secondary allocation, if capacity remains, allocate same insurance company rows
  - Checks "Insurance List" column for capabilities before allocation
  - Only allocates rows from the same insurance company as assigned from secondary allocation

**Steps:**

- Step 3.6 Phase 1 (Secondary Insurance Allocation)
- Step 3.6 Phase 2 (Same Insurance Company Rows)
- Step 3.6.5 (Global Sec + Single - Same Insurance Rows)

---

### 7. "Sec + Mix"

**Allocation Rule:**

- **Phase 1:** Allocate secondary insurance rows first (from "Dental Secondary Ins Carr" column)
- **Phase 2:** If capacity remains, allocate multiple insurance company rows (Mix logic)
- Logic:
  - Phase 1: Secondary insurance rows are allocated in Step 3.6 (Global Secondary Insurance Allocation)
  - Phase 2: After secondary allocation, if capacity remains, allocate multiple insurance company rows
  - Checks "Insurance List" column for capabilities before allocation
  - Can allocate rows from different insurance companies (unlike "Sec + Single")

**Steps:**

- Step 3.6 Phase 1 (Secondary Insurance Allocation)
- Step 3.6 Phase 2 (Multiple Insurance Company Rows)

---

### 8. "Sec + NTC"

**Allocation Rule:**

- **Phase 1:** Allocate secondary insurance rows first (from "Dental Secondary Ins Carr" column)
- **Phase 2:** If capacity remains, allocate NTC remark rows
- Logic:
  - Phase 1: Secondary insurance rows are allocated in Step 3.6 (Global Secondary Insurance Allocation)
  - Phase 2: After secondary allocation, if capacity remains, allocate NTC remark rows

**Steps:**

- Step 3.6 Phase 1 (Secondary Insurance Allocation)
- Step 3.6 Phase 2 (NTC Rows)

---

### 9. "Sec + Mix + NTC"

**Allocation Rule:**

- **Phase 1:** Allocate secondary insurance rows first (from "Dental Secondary Ins Carr" column)
- **Phase 2:** If capacity remains, allocate mixed insurance company rows
- **Phase 3:** If additional capacity remains, allocate NTC remark rows
- Logic:
  - Phase 1: Secondary insurance rows are allocated in Step 3.6 (Global Secondary Insurance Allocation)
  - Phase 2: After secondary allocation, if capacity remains, allocate multiple insurance company rows
  - Phase 3: After Phase 2, if capacity still remains, allocate NTC remark rows
  - Checks "Insurance List" column for capabilities before allocation in Phase 2

**Steps:**

- Step 3.6 Phase 1 (Secondary Insurance Allocation)
- Step 3.6 Phase 2 (Multiple Insurance Company Rows)
- Step 3.6 Phase 3 (NTC Rows)

---

## Allocation Order Summary

The system processes allocations in the following order:

1. **Step 2.5:** Global NTBP Allocation (PB preference agents)
2. **Step 3.5:** Global NTC Allocation (NTC preference agents)
3. **Step 3.5.5:** Mix + NTC - Other Insurance Rows (Mix + NTC agents)
4. **Step 3.6:** Global Secondary Insurance Allocation (Sec + X agents)
   - Phase 1: Secondary insurance rows
   - Phase 2: Based on "X" (Single, Mix, NTC, Mix + NTC)
5. **Step 3.6.5:** Global Sec + Single - Same Insurance Rows (Sec + Single agents)
6. **Step 3.7:** Global Single Allocation (Single preference agents)
7. **Step 3.8:** Global Mix Allocation (Mix preference agents)
8. **Step 3:** First Priority matched work to senior agents
9. **Step 4:** Unmatched insurance companies to senior agents
10. **Step 5:** Fallback allocation

---

## Key Rules

### Insurance List Capabilities Check

- All allocation logic checks the "Insurance List" column to verify agent capabilities
- Senior agents can work with any insurance company
- Non-senior agents can only work with insurance companies listed in their "Insurance List" column
- Matching is case-insensitive and supports partial matches

### Capacity Management

- Uses "CC" column for current capacity (falls back to "TFD" or "Capacity" if "CC" not available)
- Allocations continue until agent capacity is full or no more rows are available
- Capacity is checked before each allocation phase

### Row Exclusions

- NTBP rows: Only allocated to PB preference agents (Step 2.5)
- NTC rows: Only allocated to NTC preference agents (Step 3.5 or in Sec + NTC/Mix + NTC logic)
- Secondary insurance rows: Prioritized for "Sec + X" agents (Step 3.6)
- Already allocated rows: Skipped in all subsequent steps

### Role-Based Filtering

- Uses "Category" column for agent roles (Senior, Auditor, Junior, Trainee)
- "Auditor" role agents do not receive any work allocation
- Senior agents have priority in certain allocation steps

---

## Notes

- All allocation preferences are case-insensitive
- Multiple preferences can be combined (e.g., "Sec + Mix + NTC")
- The system processes allocations in a specific order to ensure proper distribution
- Each agent's "Insurance List" capabilities are checked before allocation
- Capacity is tracked and updated throughout the allocation process
