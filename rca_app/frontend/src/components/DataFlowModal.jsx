import React from 'react';
import { X } from 'lucide-react';

/*
  DAG layout (left-to-right):

  Column 0 (Sources)     Col 1 (Bronze)    Col 2 (Silver)    Col 3 (Gold)    Col 4 (Lakebase)   Col 5 (App)
  ┌─────────────────┐
  │ Zerobus OTel    │──┐
  └─────────────────┘  │
  ┌─────────────────┐  │  ┌───────────┐   ┌───────────┐   ┌──────────┐   ┌────────────┐   ┌──────────────┐
  │ Kafka Streaming │──┼─▶│  Bronze   │──▶│  Silver   │──▶│   Gold   │──▶│  Lakebase  │──▶│  RCA App     │
  └─────────────────┘  │  └───────────┘   └───────────┘   └──────────┘   └────────────┘   └──────────────┘
  ┌─────────────────┐  │
  │ Autoloader (S3) │──┘
  └─────────────────┘
*/

const SOURCES = [
  { id: 'zerobus', label: 'Zerobus OTel', sub: 'Metrics, Logs, Traces', color: '#4a90e2' },
  { id: 'kafka', label: 'Kafka Streaming', sub: 'Events, Alerts, Changes', color: '#f59e42' },
  { id: 'autoloader', label: 'Autoloader (S3)', sub: 'ServiceNow, Splunk', color: '#34d399' },
];

const PIPELINE = [
  {
    id: 'bronze', label: 'Bronze', sub: 'Raw Ingestion',
    color: '#f59e42',
    tables: ['bronze_metrics', 'bronze_logs', 'bronze_traces', 'bronze_events', 'bronze_network_flows'],
  },
  {
    id: 'silver', label: 'Silver', sub: 'Enriched & Joined',
    color: '#00d4ff',
    tables: ['silver_incidents', 'silver_alerts', 'silver_changes', 'silver_service_health', 'silver_servicenow'],
  },
  {
    id: 'gold', label: 'Gold', sub: 'Analytics Layer',
    color: '#34d399',
    tables: ['gold_root_cause_patterns', 'gold_service_risk_ranking', 'gold_business_impact'],
  },
  {
    id: 'lakebase', label: 'Lakebase', sub: 'PostgreSQL Sync',
    color: '#bc8cff',
    tables: ['12 synced tables', 'eo_analytics schema', 'OAuth auth'],
  },
  {
    id: 'app', label: 'RCA App', sub: 'FastAPI + React',
    color: '#ff6b9d',
    tables: ['Dashboards', 'Genie NLQ', 'Service Map'],
  },
];

const NODE = {
  background: '#0d1b2a',
  borderRadius: 10,
  padding: '12px 16px',
  minWidth: 160,
};

const ARROW_COLOR = '#1e3a5f';

function DagNode({ label, sub, color, tables }) {
  return (
    <div style={{
      ...NODE,
      border: `1px solid ${color}50`,
      borderLeft: `3px solid ${color}`,
    }}>
      <div style={{ fontSize: '0.88rem', fontWeight: 700, color: '#e8edf4' }}>{label}</div>
      <div style={{ fontSize: '0.7rem', color: '#5a7a9e', marginTop: 2 }}>{sub}</div>
      {tables && (
        <div style={{ marginTop: 8, borderTop: '1px solid #1e3a5f', paddingTop: 6 }}>
          {tables.map((t, i) => (
            <div key={i} style={{
              fontSize: '0.68rem', color: color, fontFamily: "'SF Mono', 'Consolas', monospace",
              padding: '1px 0',
            }}>
              {t}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function Arrow({ color = ARROW_COLOR }) {
  return (
    <div style={{ display: 'flex', alignItems: 'center', minWidth: 36, position: 'relative' }}>
      <div style={{
        height: 2, flex: 1,
        background: `linear-gradient(90deg, ${color}80, ${color})`,
        position: 'relative', overflow: 'hidden',
      }}>
        <div style={{
          position: 'absolute', top: -2, width: 6, height: 6, borderRadius: '50%',
          background: color, animation: 'df-flow 2s linear infinite',
        }} />
        <div style={{
          position: 'absolute', top: -2, width: 6, height: 6, borderRadius: '50%',
          background: color, opacity: 0.5, animation: 'df-flow 2s linear infinite 0.7s',
        }} />
      </div>
      <div style={{
        width: 0, height: 0,
        borderTop: '5px solid transparent', borderBottom: '5px solid transparent',
        borderLeft: `7px solid ${color}`,
      }} />
    </div>
  );
}

export default function DataFlowModal({ isOpen, onClose }) {
  if (!isOpen) return null;

  return (
    <div
      style={{
        position: 'fixed', inset: 0, zIndex: 9999,
        background: 'rgba(5, 10, 20, 0.85)',
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        backdropFilter: 'blur(4px)',
      }}
      onClick={(e) => { if (e.target === e.currentTarget) onClose(); }}
    >
      <div style={{
        background: '#0a1628',
        border: '1px solid #1e3a5f',
        borderRadius: 16,
        maxWidth: 1400,
        width: '96vw',
        maxHeight: '90vh',
        overflow: 'auto',
        padding: '28px 32px',
        boxShadow: '0 20px 60px rgba(0,0,0,0.6)',
      }}>
        {/* Header */}
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 28 }}>
          <div>
            <h2 style={{ fontSize: '1.3rem', fontWeight: 700, color: '#e8edf4', margin: 0 }}>
              Telemetry Data Flow
            </h2>
            <p style={{ fontSize: '0.85rem', color: '#5a7a9e', margin: '4px 0 0' }}>
              End-to-end pipeline: Telemetry Sources → Medallion Architecture → Lakebase → App
            </p>
          </div>
          <button
            onClick={onClose}
            style={{
              background: '#111f33', border: '1px solid #1e3a5f', borderRadius: 8,
              color: '#8ba3c1', padding: '6px 8px', cursor: 'pointer',
              display: 'flex', alignItems: 'center',
            }}
          >
            <X size={16} />
          </button>
        </div>

        {/* DAG */}
        <div style={{ display: 'flex', alignItems: 'stretch', gap: 0, overflowX: 'auto' }}>

          {/* Column 0: Sources (stacked vertically) */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: 12, justifyContent: 'center' }}>
            {SOURCES.map((src) => (
              <DagNode key={src.id} label={src.label} sub={src.sub} color={src.color} />
            ))}
          </div>

          {/* Fan-in arrows from sources to Bronze */}
          <div style={{ display: 'flex', flexDirection: 'column', justifyContent: 'center', position: 'relative', minWidth: 50 }}>
            <svg width="50" height="200" viewBox="0 0 50 200" style={{ display: 'block' }}>
              {/* Three lines converging from left edge to right center */}
              <line x1="0" y1="35" x2="42" y2="100" stroke="#f59e4280" strokeWidth="2" />
              <line x1="0" y1="100" x2="42" y2="100" stroke="#f59e4280" strokeWidth="2" />
              <line x1="0" y1="165" x2="42" y2="100" stroke="#f59e4280" strokeWidth="2" />
              {/* Arrow head */}
              <polygon points="42,95 42,105 50,100" fill="#f59e42" opacity="0.6" />
              {/* Animated dots */}
              <circle r="3" fill="#f59e42">
                <animateMotion dur="2s" repeatCount="indefinite" path="M0,35 L42,100" />
              </circle>
              <circle r="3" fill="#f59e42" opacity="0.6">
                <animateMotion dur="2s" repeatCount="indefinite" begin="0.3s" path="M0,100 L42,100" />
              </circle>
              <circle r="3" fill="#f59e42" opacity="0.4">
                <animateMotion dur="2s" repeatCount="indefinite" begin="0.6s" path="M0,165 L42,100" />
              </circle>
            </svg>
          </div>

          {/* Columns 1–5: Pipeline stages with arrows between */}
          {PIPELINE.map((stage, idx) => (
            <React.Fragment key={stage.id}>
              <div style={{ display: 'flex', alignItems: 'center' }}>
                <DagNode label={stage.label} sub={stage.sub} color={stage.color} tables={stage.tables} />
              </div>
              {idx < PIPELINE.length - 1 && (
                <div style={{ display: 'flex', alignItems: 'center' }}>
                  <Arrow color={PIPELINE[idx + 1].color} />
                </div>
              )}
            </React.Fragment>
          ))}
        </div>

        {/* Column labels */}
        <div style={{
          display: 'flex', gap: 0, marginTop: 16, paddingTop: 12,
          borderTop: '1px solid #1e3a5f18',
        }}>
          {['Sources', '', 'Bronze', '', 'Silver', '', 'Gold', '', 'Lakebase', '', 'App'].map((label, i) => (
            label ? (
              <div key={i} style={{
                flex: 1, textAlign: 'center', fontSize: '0.68rem',
                color: '#5a7a9e', textTransform: 'uppercase', letterSpacing: '0.08em', fontWeight: 600,
              }}>
                {label}
              </div>
            ) : <div key={i} style={{ minWidth: 36 }} />
          ))}
        </div>

        <style>{`
          @keyframes df-flow {
            0% { left: -6px; opacity: 0; }
            10% { opacity: 1; }
            90% { opacity: 1; }
            100% { left: 100%; opacity: 0; }
          }
        `}</style>
      </div>
    </div>
  );
}
