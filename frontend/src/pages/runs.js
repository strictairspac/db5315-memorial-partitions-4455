import {heading, panel} from '../components/common/heading.js';
import {records} from '../components/common/records.js';
import {badge} from '../components/common/status.js';
import {activeStatuses, statusLabels} from '../constants/workflow.js';
import {topic} from '../constants/project.js';
import {date, esc} from '../utils/dom.js';

const monthText = months => Array.isArray(months) && months.length ? months.join('、') : '—';

export function runsPage(state, identity) {
  const selected = state.runs.find(row=>row.id===identity) || state.runs[0];
  const cases = new Map(state.cases.map(row=>[row.id, row.title]));
  const rows = state.runs.map(row=>[`<a href="#runs/${row.id}">${esc(cases.get(row.case_id) || row.case_id)}</a>`, badge(row.status),
    esc(monthText(row.months)), esc(row.status==='waiting' ? monthText(row.waiting_for) : '—'), esc(row.owner), esc(date(row.created_at))]);
  const result = state.evidence.find(row=>row.run_id===selected?.id);
  const active = selected && activeStatuses.includes(selected.status);
  const waiting = selected?.status==='waiting';
  const detail = selected ? `<h2>${esc(cases.get(selected.case_id) || selected.case_id)}</h2>
    <p data-testid="run-status">${badge(selected.status)}</p>
    ${waiting?`<p class="note" data-testid="waiting-months">正在等待月份：${esc(monthText(selected.waiting_for))}。等待时不占用其他月份。</p>`:''}
    <dl><dt>涉及月份</dt><dd>${esc(monthText(selected.months))}</dd><dt>启动时间</dt><dd>${esc(date(selected.started_at))}</dd><dt>完成时间</dt><dd>${esc(date(selected.finished_at))}</dd></dl>
    ${selected.error ? `<p class="error">${esc(selected.error)}</p>` : ''}
    ${active?'<p class="note">后台处理中，记录会自动刷新。</p>':''}
    ${result?`<a class="button" href="#evidence/${result.id}">查看本批证据</a>`:''}` : '<h2>执行记录</h2><p>从档案创建一条执行记录后，可在此查看进度。</p>';
  const audit = state.audit.filter(x=>selected && x.reference===String(selected.id) && ['enqueue', 'wait', 'grant', 'recover', 'complete', 'failed'].includes(x.action));
  return `${heading(topic.modules[2], `${state.spec.entity_labels[1]}与后台处理进度`, '<button class="secondary" type="button" id="refresh">刷新记录</button>')}
    <div class="spread"><section>${records(['档案','状态','涉及月份','等待月份','经办','提交时间'], rows, '暂无执行记录。')}</section><aside class="brief">${detail}</aside></div>
    ${audit.length?panel('操作记录', records(['操作者','操作','时间'], audit.map(x=>[esc(x.actor),esc(statusLabels[x.action]||'提交执行'),esc(date(x.created_at))]))):''}`;
}
