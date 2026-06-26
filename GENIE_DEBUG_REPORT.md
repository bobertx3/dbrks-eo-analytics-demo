# Ask Genie Page Debug Report

## Summary
The "Ask Genie" page has a **critical bug in the response parsing logic** in the backend. The Genie Space API is working correctly and returning proper data, but the backend code fails to extract it from the response, resulting in empty answers being sent to the frontend.

## Issue Details

### Symptom
When asking "What are the top incidents?" or similar questions:
- The API returns a `conversation_id` and `message_id`
- The answer field shows the question echoed back: `"What are the top incidents?"`
- No SQL query is shown
- No data table is displayed

### Root Cause
The backend `_extract_genie_answer()` function in `rca_app/backend/routes/genie.py` (lines 140-164) is looking for attachments with a `"type"` field that doesn't exist in the actual Genie Space API response.

**Current (broken) code:**
```python
for att in attachments:
    if att.get("type") == "TEXT":         # ← Checking for "type" field
        answer_text = att.get("text", {}).get("content", "")
    elif att.get("type") == "QUERY":      # ← This will never match
        query_info = att.get("query", {})
```

**Actual response structure:**
The Genie Space returns attachments as objects with keys like `"query"`, `"text"`, `"suggested_questions"`, NOT a `"type"` field:

```json
{
  "attachments": [
    {
      "query": {
        "query": "SELECT ...",
        "statement_id": "01f12b0f-711a-122c-a895-b1b37adf0b41",
        "query_result_metadata": {"row_count": 13}
      },
      "attachment_id": "01f12b0f710c1198bbf59024cf5c5caa"
    },
    {
      "text": {
        "content": "The top incidents are all classified as P2 severity..."
      }
    },
    {
      "suggested_questions": {
        "questions": [...]
      }
    }
  ]
}
```

### Fallback Mechanism
When parsing fails, the code attempts to use `data.get("content")` (line 162), which returns the user's original question, not an answer. This is why the question is echoed back.

## API Response Details

### Sample Request
```
POST /api/2.0/genie/spaces/01f12b09e924156a8dd45ba78787ed15/start-conversation
Content-Type: application/json

{"content": "What are the top incidents?"}
```

### Sample Response (COMPLETED status)
```json
{
  "status": "COMPLETED",
  "content": "What are the top incidents?",
  "query_result": {
    "statement_id": "01f12b0f-711a-122c-a895-b1b37adf0b41",
    "row_count": 13
  },
  "attachments": [
    {
      "query": {
        "query": "WITH ranked_incidents AS (\n  SELECT *,\n    RANK() OVER (ORDER BY severity DESC, impact_score DESC) AS rank\n  FROM `bldemos`.`eo_analytics`.`silver_incidents`\n  WHERE severity IS NOT NULL AND impact_score IS NOT NULL\n)\nSELECT incident_id, title, severity, impact_score, created_at, status\nFROM ranked_incidents\nWHERE rank <= 10",
        "description": "You want to see the top 10 incidents...",
        "statement_id": "01f12b0f-711a-122c-a895-b1b37adf0b41",
        "query_result_metadata": {
          "row_count": 13
        }
      },
      "attachment_id": "01f12b0f710c1198bbf59024cf5c5caa"
    },
    {
      "text": {
        "content": "The top incidents are all classified as **P2 severity** and are resolved. Notable data points include:\n- **INC-1001**: ERP SAP Connector Batch Sync Overload, impact score 1080.0\n- **INC-1009**: ERP SAP Connector Batch Sync Overload, impact score 1080.0\n...",
      }
    },
    {
      "suggested_questions": {
        "questions": [
          "What are the incidents with the highest revenue impact?",
          "Which incidents have the longest mean time to recovery (MTTR)?",
          "What are the incidents with the largest blast radius?"
        ]
      },
      "attachment_id": "01f12b0f727d171581f16b969456e52b"
    },
    {
      "text": {
        "content": "Would you prefer to see the top incidents ranked by severity and impact score, or by other criteria..."
      }
    }
  ]
}
```

### Status Progression
- Initial: `SUBMITTED`
- During processing: `ASKING_AI`, `PENDING_WAREHOUSE`
- Final: `COMPLETED` (or `FAILED`)

### Response Time
- First status check (2 seconds): `ASKING_AI`
- Subsequent checks (every 2 seconds): progresses through `PENDING_WAREHOUSE` → `ASKING_AI` → `COMPLETED`
- Total time: ~8 seconds from submission to completion

## Data Available

### Query Data
The Genie Space successfully executes SQL queries and returns:
- **SQL**: The generated SQL query string
- **Statement ID**: For fetching detailed results via `/api/2.0/genie/queries/{statement_id}`
- **Row Count**: Available in `query_result_metadata.row_count` and `query_result.row_count`

Note: The actual query result rows are NOT included in the message response. They must be fetched separately using the `statement_id` with the query results endpoint (if needed for detailed data export).

### Analysis
- **TEXT attachments**: Contain the LLM-generated narrative analysis of the data
- **Suggested Questions**: Contextual follow-up questions for multi-turn conversations

## Lakebase Connection Status
The backend fallback mechanism is working correctly:
- Lakebase PostgreSQL connection: ✓ OPERATIONAL
- Synced tables available in PostgreSQL:
  - `silver_incidents` (30 rows) ✓
  - `gold_business_impact_summary` (5 rows) ✓
  - `gold_root_cause_patterns` (10 rows) ✓
  - `silver_servicenow_correlation` (30 rows) ✓

Fallback would work if Genie parsing fails, but the Genie API is not the issue—the parsing is.

## Files Affected
- **Primary**: `/Users/robert.leach/dev/vibe/dbrks-eo-analytics-demo/rca_app/backend/routes/genie.py`
  - Function: `_extract_genie_answer()` (lines 140-164)
  - Function: `_poll_genie_result()` (lines 108-132)

## Fix Required
Rewrite `_extract_genie_answer()` to:
1. Check for `"query"` key in attachment (not `"type"`)
2. Check for `"text"` key in attachment (not `"type"`)
3. Extract text content from `att.get("text", {}).get("content", "")`
4. Extract SQL from `att.get("query", {}).get("query", "")`
5. Collect all TEXT attachments (there can be multiple) and combine them for the answer
6. For data retrieval, either:
   - Use `query_result.row_count` to show row count
   - Fetch actual query results separately if needed (requires additional API call)

### Example of Fixed Logic
```python
def _extract_genie_answer(data):
    """Extract the answer text, SQL, and data from a Genie response."""
    attachments = data.get("attachments", [])
    answer_texts = []
    sql_query = None

    for att in attachments:
        # Check for TEXT content (answer)
        if "text" in att:
            text_content = att.get("text", {}).get("content", "")
            if text_content:
                answer_texts.append(text_content)

        # Check for QUERY (SQL and results)
        elif "query" in att:
            query_info = att.get("query", {})
            sql_query = query_info.get("query", "")
            # Note: actual row data is not in the response;
            # row count is in query_result_metadata.row_count

    answer_text = "\n\n".join(answer_texts) or data.get("content", "No answer text available.")

    return {"answer": answer_text, "sql": sql_query, "data": None}
```

## Verification
To verify the fix works:
1. Navigate to http://localhost:5173/genie
2. Ask "What are the top incidents?"
3. Verify the response shows:
   - Answer text: Multi-paragraph narrative (not the question echoed back)
   - SQL: The actual SQL query that was generated
   - Data: Can remain None or be populated after fetching query results separately
