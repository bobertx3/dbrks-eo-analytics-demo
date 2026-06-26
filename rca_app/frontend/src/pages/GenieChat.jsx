import React, { useState, useRef, useEffect } from 'react';
import { Send, Sparkles, ChevronDown, ChevronRight, Database, Loader2 } from 'lucide-react';
import { formatCurrency, formatNumber } from '../hooks/useApi';

const STARTER_QUESTIONS = [
  {
    label: 'Supply Chain Disruption',
    question: 'What was a recent P1 event that caused delays in shipment of goods?',
    color: '#f0883e',
    icon: '\u{1F4E6}',
    description: 'Inventory API failures cascading to order fulfillment',
  },
  {
    label: 'Digital Surgery Productivity',
    question: 'What was a recent P1 event impacting digital surgery data scientists?',
    color: '#bc8cff',
    icon: '\u{1F9EC}',
    description: 'SageMaker VPC routing misconfiguration impacting ML workloads',
  },
  {
    label: 'Duplicate ServiceNow Tickets',
    question: 'Which services have the most duplicate ServiceNow tickets?',
    color: '#58a6ff',
    icon: '\u{1F3AB}',
    description: 'Identify ticket noise and automation opportunities',
  },
  {
    label: 'Revenue at Risk',
    question: 'What is the total revenue at risk from supply chain incidents this quarter?',
    color: '#f85149',
    icon: '\u{1F4B0}',
    description: 'Financial impact analysis across business units',
  },
  {
    label: 'Blast Radius Analysis',
    question: 'Which root cause pattern has the highest blast radius across all domains?',
    color: '#39d353',
    icon: '\u{1F4A5}',
    description: 'Cascading failure patterns affecting multiple services',
  },
  {
    label: 'Data Scientist Impact',
    question: 'How many data scientists were impacted by infrastructure incidents in the last 90 days?',
    color: '#d29922',
    icon: '\u{1F468}\u200D\u{1F4BB}',
    description: 'Productivity loss in the Digital Surgery division',
  },
];

function MarkdownText({ text }) {
  // Simple markdown-like rendering for bold and line breaks
  if (!text) return null;
  const parts = text.split(/(\*\*[^*]+\*\*)/g);
  return (
    <span>
      {parts.map((part, i) => {
        if (part.startsWith('**') && part.endsWith('**')) {
          return <strong key={i}>{part.slice(2, -2)}</strong>;
        }
        // Handle newlines
        const lines = part.split('\n');
        return lines.map((line, j) => (
          <React.Fragment key={`${i}-${j}`}>
            {j > 0 && <br />}
            {line}
          </React.Fragment>
        ));
      })}
    </span>
  );
}

function DataTable({ data }) {
  if (!data || data.length === 0) return null;
  const columns = Object.keys(data[0]);

  return (
    <div className="genie-data-table-wrapper">
      <table className="genie-data-table">
        <thead>
          <tr>
            {columns.map((col) => (
              <th key={col}>{col.replace(/_/g, ' ')}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.slice(0, 20).map((row, idx) => (
            <tr key={idx}>
              {columns.map((col) => {
                let val = row[col];
                // Format numbers and currencies
                if (col.includes('usd') || col.includes('revenue') || col.includes('cost') || col.includes('loss')) {
                  val = formatCurrency(val);
                } else if (typeof val === 'number' || (typeof val === 'string' && !isNaN(val) && val !== '')) {
                  const num = Number(val);
                  if (!isNaN(num) && num > 1000) {
                    val = formatNumber(val);
                  }
                }
                // Truncate long strings
                if (typeof val === 'string' && val.length > 120) {
                  val = val.substring(0, 117) + '...';
                }
                // Handle arrays
                if (Array.isArray(val)) {
                  val = val.join(', ');
                }
                return <td key={col}>{val === null || val === undefined ? '--' : String(val)}</td>;
              })}
            </tr>
          ))}
        </tbody>
      </table>
      {data.length > 20 && (
        <div style={{ padding: '8px 12px', fontSize: '0.75rem', color: 'var(--color-text-muted)' }}>
          Showing 20 of {data.length} rows
        </div>
      )}
    </div>
  );
}

function ChatMessage({ message }) {
  const [showSql, setShowSql] = useState(false);
  const [showData, setShowData] = useState(true);

  if (message.role === 'user') {
    return (
      <div className="chat-message user-message">
        <div className="chat-bubble user-bubble">
          {message.content}
        </div>
      </div>
    );
  }

  return (
    <div className="chat-message assistant-message">
      <div className="chat-bubble assistant-bubble">
        <div className="chat-answer-text">
          <MarkdownText text={message.content} />
        </div>

        {message.sql && (
          <div className="chat-sql-section">
            <button
              className="chat-toggle-btn"
              onClick={() => setShowSql(!showSql)}
            >
              <Database size={14} />
              {showSql ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
              View SQL Query
            </button>
            {showSql && (
              <pre className="chat-sql-code">{message.sql}</pre>
            )}
          </div>
        )}

        {message.data && message.data.length > 0 && (
          <div className="chat-data-section">
            <button
              className="chat-toggle-btn"
              onClick={() => setShowData(!showData)}
            >
              {showData ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
              Supporting Data ({message.data.length} rows)
            </button>
            {showData && <DataTable data={message.data} />}
          </div>
        )}
      </div>
    </div>
  );
}

export default function GenieChat() {
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const [conversationId, setConversationId] = useState(null);
  const messagesEndRef = useRef(null);
  const inputRef = useRef(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const sendQuestion = async (question) => {
    if (!question.trim() || loading) return;

    const userMsg = { role: 'user', content: question };
    setMessages((prev) => [...prev, userMsg]);
    setInput('');
    setLoading(true);

    try {
      const resp = await fetch('/api/genie/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          question,
          conversation_id: conversationId,
        }),
      });

      if (!resp.ok) {
        throw new Error(`HTTP ${resp.status}`);
      }

      const data = await resp.json();

      if (data.conversation_id) {
        setConversationId(data.conversation_id);
      }

      const assistantMsg = {
        role: 'assistant',
        content: data.answer || 'No answer returned.',
        sql: data.sql || null,
        data: data.data || null,
      };
      setMessages((prev) => [...prev, assistantMsg]);
    } catch (err) {
      const errorMsg = {
        role: 'assistant',
        content: `Error querying: ${err.message}. Please try again.`,
        sql: null,
        data: null,
      };
      setMessages((prev) => [...prev, errorMsg]);
    } finally {
      setLoading(false);
      inputRef.current?.focus();
    }
  };

  const handleSubmit = (e) => {
    e.preventDefault();
    sendQuestion(input);
  };

  const handleStarterClick = (question) => {
    sendQuestion(question);
  };

  const startNewChat = () => {
    setMessages([]);
    setConversationId(null);
    setInput('');
    inputRef.current?.focus();
  };

  const hasMessages = messages.length > 0;

  return (
    <div className="genie-chat-page">
      {/* Header — compact when chatting */}
      <div className="genie-header">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <h2 style={{ fontSize: '1rem', margin: 0 }}>
            <Sparkles size={18} style={{ verticalAlign: 'middle', marginRight: 8, color: 'var(--color-accent)' }} />
            Ask Genie
          </h2>
          {hasMessages && (
            <button
              type="button"
              className="btn btn-secondary"
              onClick={startNewChat}
              disabled={loading}
              style={{ fontSize: '0.75rem', padding: '6px 14px' }}
            >
              New Chat
            </button>
          )}
        </div>
      </div>

      {/* Empty state — centered welcome */}
      {!hasMessages && (
        <div className="genie-welcome">
          <div className="genie-welcome-inner">
            <Sparkles size={36} style={{ color: 'var(--color-accent)', marginBottom: 12 }} />
            <h2 className="genie-welcome-title">What do you want to investigate?</h2>
            <p className="genie-welcome-subtitle">
              Ask about incidents, root causes, and business impact across Dbrks domains
            </p>
          </div>
        </div>
      )}

      {/* Chat messages */}
      {hasMessages && (
        <div className="genie-messages-container">
          {messages.map((msg, idx) => (
            <ChatMessage key={idx} message={msg} />
          ))}
          {loading && (
            <div className="chat-message assistant-message">
              <div className="chat-bubble assistant-bubble loading-bubble">
                <Loader2 size={16} className="spinning" />
                <span>Analyzing your question...</span>
              </div>
            </div>
          )}
          <div ref={messagesEndRef} />
        </div>
      )}

      {/* Suggestion chips + input bar pinned to bottom */}
      <div className="genie-input-bar">
        <div className="genie-suggestions">
          {STARTER_QUESTIONS.map((sq) => (
            <button
              key={sq.label}
              className="genie-suggestion"
              onClick={() => handleStarterClick(sq.question)}
              disabled={loading}
            >
              <span className="genie-suggestion-icon">{sq.icon}</span>
              <span className="genie-suggestion-label">{sq.label}</span>
            </button>
          ))}
        </div>
        <form onSubmit={handleSubmit} className="genie-input-form">
          <input
            ref={inputRef}
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder="Ask a question about incidents, root causes, or business impact..."
            className="genie-input"
            disabled={loading}
          />
          <button
            type="submit"
            className="genie-send-btn"
            disabled={!input.trim() || loading}
          >
            <Send size={18} />
          </button>
        </form>
      </div>
    </div>
  );
}
