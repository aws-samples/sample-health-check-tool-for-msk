"""PDF report builder.

Order of the report: executive summary, data quality and scope, action plan, findings that
need attention (by category), healthy checks, cluster inventory, methodology and references.
Every finding shows the observed value, the threshold, the confidence and the source, and
each chart carries the threshold line the check used.
"""

import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from io import BytesIO
from typing import Any, Dict, List, Optional

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (BaseDocTemplate, Frame, Image, KeepTogether, PageBreak, PageTemplate, Paragraph,
                                Spacer, Table, TableStyle)
from reportlab.platypus.tableofcontents import TableOfContents

from . import __version__
from .analyzer import CATEGORY_WEIGHTS, AnalysisResult, Category, Finding, Severity
from .cluster_info import ClusterInfo
from .manifest import redact_arn, redacted_name
from .metrics_collector import MetricData, metric_title
from .recommendations import Recommendation
from . import reference as ref

PRIMARY = colors.HexColor('#0b5ed7')
GREY = colors.HexColor('#6c757d')
LIGHT = colors.HexColor('#f1f3f5')
LINE = colors.HexColor('#dee2e6')
SEVERITY_COLORS = {
    Severity.CRITICAL: colors.HexColor('#b02a37'),
    Severity.HIGH: colors.HexColor('#c2410c'),
    Severity.WARNING: colors.HexColor('#b8860b'),
    Severity.INFORMATIONAL: colors.HexColor('#0b5ed7'),
    Severity.HEALTHY: colors.HexColor('#2e7d32'),
    Severity.NOT_ASSESSED: colors.HexColor('#6c757d'),
}
SEVERITY_FILL = {
    Severity.CRITICAL: colors.HexColor('#f8d7da'),
    Severity.HIGH: colors.HexColor('#ffe5d0'),
    Severity.WARNING: colors.HexColor('#fff3cd'),
    Severity.INFORMATIONAL: colors.HexColor('#dbe7ff'),
    Severity.HEALTHY: colors.HexColor('#d4edda'),
    Severity.NOT_ASSESSED: colors.HexColor('#e9ecef'),
}
SEVERITY_LABEL = {
    Severity.CRITICAL: 'CRITICAL', Severity.HIGH: 'HIGH', Severity.WARNING: 'WARNING', Severity.INFORMATIONAL: 'INFO',
    Severity.HEALTHY: 'HEALTHY', Severity.NOT_ASSESSED: 'NOT ASSESSED',
}
CATEGORY_TITLES = {
    Category.RELIABILITY: 'Reliability and availability', Category.PERFORMANCE: 'Performance and capacity',
    Category.SECURITY: 'Security', Category.COST: 'Cost optimisation',
}
CATEGORY_ORDER = [Category.RELIABILITY, Category.PERFORMANCE, Category.SECURITY, Category.COST]


@dataclass
class ReportContent:
    """Complete report data."""
    cluster_info: ClusterInfo
    analysis: AnalysisResult
    recommendations: List[Recommendation]
    charts: List                      # List[ChartImage]
    generation_time: datetime
    redact: bool = False
    comparison: Optional[Dict[str, Any]] = None
    manifest_filename: str = ''


# --------------------------------------------------------------------------- styles and template

def _styles() -> Dict[str, ParagraphStyle]:
    base = getSampleStyleSheet()
    s = {
        'title': ParagraphStyle('title', parent=base['Title'], fontSize=26, leading=32, textColor=PRIMARY, alignment=TA_CENTER),
        'subtitle': ParagraphStyle('subtitle', parent=base['Normal'], fontSize=12, leading=16, textColor=GREY, alignment=TA_CENTER),
        'h1': ParagraphStyle('h1', parent=base['Heading1'], fontSize=18, leading=22, textColor=PRIMARY, spaceBefore=6, spaceAfter=10),
        'h2': ParagraphStyle('h2', parent=base['Heading2'], fontSize=14, leading=18, spaceBefore=12, spaceAfter=6),
        'h2f': ParagraphStyle('h2f', parent=base['Heading2'], fontSize=13, leading=17, spaceBefore=12, spaceAfter=6),
        'h3': ParagraphStyle('h3', parent=base['Heading3'], fontSize=11.5, leading=15, spaceBefore=8, spaceAfter=4),
        'body': ParagraphStyle('body', parent=base['Normal'], fontSize=9.5, leading=13),
        'small': ParagraphStyle('small', parent=base['Normal'], fontSize=8, leading=10.5, textColor=GREY),
        'cell': ParagraphStyle('cell', parent=base['Normal'], fontSize=8, leading=10.5),
        'cellb': ParagraphStyle('cellb', parent=base['Normal'], fontSize=8, leading=10.5, fontName='Helvetica-Bold'),
        'caption': ParagraphStyle('caption', parent=base['Normal'], fontSize=8, leading=10.5, textColor=GREY, spaceBefore=2),
        'toc1': ParagraphStyle('toc1', parent=base['Normal'], fontSize=10.5, leading=15, leftIndent=0),
        'toc2': ParagraphStyle('toc2', parent=base['Normal'], fontSize=9.5, leading=13, leftIndent=14),
    }
    return s


class ReportDocTemplate(BaseDocTemplate):
    """Document template that feeds the table of contents and PDF outline from headings."""

    def __init__(self, filename: str, footer_text: str, **kwargs):
        super().__init__(filename, **kwargs)
        self.footer_text = footer_text
        frame = Frame(self.leftMargin, self.bottomMargin, self.width, self.height, id='main')
        self.addPageTemplates([PageTemplate(id='main', frames=[frame], onPage=self._draw_footer)])

    def _draw_footer(self, canvas, doc):
        canvas.saveState()
        canvas.setFont('Helvetica', 7.5)
        canvas.setFillColor(GREY)
        canvas.drawString(doc.leftMargin, 0.5 * inch, self.footer_text)
        canvas.drawRightString(doc.leftMargin + doc.width, 0.5 * inch, f'Page {doc.page}')
        canvas.restoreState()

    def afterFlowable(self, flowable):
        if isinstance(flowable, Paragraph) and flowable.style.name in ('h1', 'h2', 'h2f'):
            text = flowable.getPlainText()
            if text == 'Contents':
                return
            level = 0 if flowable.style.name == 'h1' else 1
            key = 'sec-' + hashlib.md5(f'{level}{text}{self.page}'.encode()).hexdigest()[:10]
            self.canv.bookmarkPage(key)
            self.canv.addOutlineEntry(text, key, level=level, closed=False)
            if flowable.style.name != 'h2f':  # finding titles go to the outline only
                self.notify('TOCEntry', (level, text, self.page, key))


# --------------------------------------------------------------------------- helpers

def _p(text: str, style: ParagraphStyle) -> Paragraph:
    return Paragraph(text, style)


def _esc(text: Any) -> str:
    return (str(text).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;'))


def _sev(sev: Severity) -> str:
    return f'<font color="{SEVERITY_COLORS[sev].hexval()}"><b>{SEVERITY_LABEL[sev]}</b></font>'


def _table(data: List[List[Any]], widths: List[float], header: bool = True, zebra: bool = True,
           row_fills: Optional[Dict[int, Any]] = None, font_size: float = 8) -> Table:
    t = Table(data, colWidths=widths, repeatRows=1 if header else 0)
    style = [
        ('GRID', (0, 0), (-1, -1), 0.5, LINE),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('FONTSIZE', (0, 0), (-1, -1), font_size),
        ('TOPPADDING', (0, 0), (-1, -1), 4), ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ('LEFTPADDING', (0, 0), (-1, -1), 5), ('RIGHTPADDING', (0, 0), (-1, -1), 5),
    ]
    if header:
        style += [('BACKGROUND', (0, 0), (-1, 0), PRIMARY), ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                  ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold')]
    if zebra:
        for i in range(1 if header else 0, len(data)):
            if (i % 2 == 0):
                style.append(('BACKGROUND', (0, i), (-1, i), LIGHT))
    for i, fill in (row_fills or {}).items():
        style.append(('BACKGROUND', (0, i), (-1, i), fill))
    t.setStyle(TableStyle(style))
    return t


def _display_name(content: ReportContent) -> str:
    return redacted_name(content.cluster_info.name) if content.redact else content.cluster_info.name


def _fmt_value(value: Optional[float], unit: str) -> str:
    if value is None:
        return '-'
    if unit in ('Percent',):
        return f'{value:.1f}%'
    if unit == 'Bytes/Second':
        return f'{value / (1024 * 1024):.2f} MB/s'
    if unit == 'Bytes':
        return f'{value / (1024 ** 3):.2f} GiB'
    if abs(value) >= 1000:
        return f'{value:,.0f}'
    if float(value).is_integer():
        return f'{value:.0f}'
    return f'{value:.2f}'


def generate_output_filename(cluster_arn: str, timestamp: datetime, extension: str = 'pdf', redact: bool = False) -> str:
    cluster_name = cluster_arn.split('/')[-2]
    if redact:
        cluster_name = redacted_name(cluster_name)
    return f'msk_health_check_{cluster_name}_{timestamp.strftime("%Y%m%d_%H%M%S")}.{extension}'


# --------------------------------------------------------------------------- sections

def _title_page(content: ReportContent, s) -> List:
    ci, an = content.cluster_info, content.analysis
    m = an.metrics
    els: List = [Spacer(1, 1.6 * inch), _p('Amazon MSK', s['title']), _p('Health Check Report', s['title']), Spacer(1, 0.3 * inch),
                 _p(f'Cluster <b>{_esc(_display_name(content))}</b> ({ci.cluster_type.title()} brokers, {_esc(ci.instance_type)} x {ci.broker_count}, Kafka {_esc(ci.kafka_version)})', s['subtitle']),
                 _p(f'Region {ci.region}' + ('' if content.redact else f', account {ci.account_id}'), s['subtitle']),
                 Spacer(1, 0.3 * inch)]
    color = SEVERITY_COLORS[{'Critical': Severity.CRITICAL, 'Needs Attention': Severity.WARNING}.get(an.overall_status, Severity.HEALTHY)]
    els.append(_p(f'<font color="{color.hexval()}"><b>{an.overall_status}</b></font> &nbsp;&nbsp; health score {an.overall_health_score:.0f}/100',
                  ParagraphStyle('status', parent=s['title'], fontSize=18, leading=24)))
    els.append(Spacer(1, 0.3 * inch))
    days = (m.end_time - m.start_time).total_seconds() / 86400
    els.append(_p(f'Metrics window {m.start_time.strftime("%Y-%m-%d %H:%M")} to {m.end_time.strftime("%Y-%m-%d %H:%M")} UTC '
                  f'({days:.1f} days, {m.period_seconds // 60}-minute buckets). Generated {content.generation_time.strftime("%Y-%m-%d %H:%M")} UTC '
                  f'by msk-health-check {__version__}, rules {ref.RULES_VERSION}.', s['subtitle']))
    els.append(Spacer(1, 1.2 * inch))
    els.append(_p('This report is an automated review against published Amazon MSK best practices and quotas. It reads '
                  'CloudWatch metrics and the cluster configuration; it does not see topics, clients or business '
                  'requirements. Treat every recommendation as a hypothesis to confirm against your workload, and note '
                  'the confidence and source shown with each finding.', s['small']))
    els.append(PageBreak())
    return els


def _toc(s) -> List:
    toc = TableOfContents()
    toc.levelStyles = [s['toc1'], s['toc2']]
    toc.dotsMinLevel = 0
    return [_p('Contents', s['h1']), toc, PageBreak()]


def _executive_summary(content: ReportContent, s) -> List:
    an, ci = content.analysis, content.cluster_info
    els: List = [_p('1. Executive summary', s['h1'])]
    counts = {sev: sum(1 for f in an.findings if f.severity == sev) for sev in Severity}
    status_color = SEVERITY_COLORS[{'Critical': Severity.CRITICAL, 'Needs Attention': Severity.WARNING}.get(an.overall_status, Severity.HEALTHY)]
    els.append(_p(f'Overall status <font color="{status_color.hexval()}"><b>{an.overall_status}</b></font>, health score '
                  f'<b>{an.overall_health_score:.0f}/100</b>. {an.checks_assessed} of {an.checks_total} checks were assessed: '
                  f'{counts[Severity.CRITICAL]} critical, {counts[Severity.HIGH]} high, {counts[Severity.WARNING]} warning, '
                  f'{counts[Severity.INFORMATIONAL]} informational, {counts[Severity.HEALTHY]} healthy; {counts[Severity.NOT_ASSESSED]} '
                  f'could not be evaluated (see section 2). The status is bounded by the worst finding: Critical means an operational '
                  f'impact now or imminent; high findings are posture or resilience gaps that call for attention without making the '
                  f'cluster unhealthy.', s['body']))
    els.append(Spacer(1, 6))
    rows = [['Category', 'Weight', 'Score', 'Critical', 'High', 'Warning', 'Not assessed']]
    for cat in CATEGORY_ORDER:
        fs = [f for f in an.findings if f.category == cat]
        rows.append([CATEGORY_TITLES[cat], f'{int(round(100 * CATEGORY_WEIGHTS[cat]))}%',
                     f'{an.category_scores.get(cat.value, 100):.0f}',
                     str(sum(1 for f in fs if f.severity == Severity.CRITICAL)),
                     str(sum(1 for f in fs if f.severity == Severity.HIGH)),
                     str(sum(1 for f in fs if f.severity == Severity.WARNING)),
                     str(sum(1 for f in fs if f.severity == Severity.NOT_ASSESSED))])
    els.append(_table(rows, [2.1 * inch, 0.7 * inch, 0.7 * inch, 0.8 * inch, 0.7 * inch, 0.8 * inch, 1.0 * inch]))
    els.append(Spacer(1, 10))

    attention = [f for f in an.findings if f.severity in (Severity.CRITICAL, Severity.HIGH, Severity.WARNING)]
    els.append(_p('Findings that need attention', s['h2']))
    if attention:
        rows = [['Severity', 'Finding', 'Observed', 'Threshold', 'Brokers']]
        fills = {}
        for i, f in enumerate(attention, start=1):
            rows.append([_p(_sev(f.severity), s['cell']), _p(_esc(f.title), s['cell']), _p(_esc(f.observed or '-'), s['cell']),
                         _p(_esc(f.threshold or '-'), s['cell']), _p(_esc(', '.join(f.affected_brokers) or '-'), s['cell'])])
            fills[i] = SEVERITY_FILL[f.severity]
        els.append(_table(rows, [0.9 * inch, 2.6 * inch, 1.5 * inch, 1.3 * inch, 0.7 * inch], zebra=False, row_fills=fills))
    else:
        els.append(_p('No critical or warning findings in the analysed window.', s['body']))
    els.append(Spacer(1, 10))

    mon = next((f for f in an.findings if f.check_id == 'enhanced_monitoring'), None)
    if mon is not None and mon.severity == Severity.INFORMATIONAL:
        left = mon.evidence.get('checks_not_assessed_because_of_level') or []
        els.append(_p('Monitoring level', s['h2']))
        els.append(_p(f'The cluster publishes DEFAULT-level metrics only. Not evaluated in this report because of that: '
                      f'{_esc("; ".join(left)) if left else "nothing in this run"}. The recommendation is to enable PER_BROKER '
                      f'(one level above DEFAULT); section 4 lists what it adds.', s['body']))
        els.append(Spacer(1, 6))
    top = content.recommendations[:3]
    if top:
        els.append(_p('First actions', s['h2']))
        for i, r in enumerate(top, 1):
            els.append(_p(f'{i}. <b>{_esc(r.priority_label)}</b> - {_esc(r.action)} <font color="{GREY.hexval()}">({_esc(r.finding.title)})</font>', s['body']))
            els.append(Spacer(1, 3))
    if content.comparison:
        c = content.comparison
        els.append(_p('Change since the previous run', s['h2']))
        prev_when = str(c.get('previous_generated_at') or '')[:16].replace('T', ' ')
        els.append(_p(f'Previous run {prev_when} UTC: status {_esc(c.get("previous_status"))}, score {c.get("previous_score")}. '
                      f'Score change {c.get("score_delta"):+.1f}. New: {len(c.get("new", []))}, resolved: {len(c.get("resolved", []))}, '
                      f'changed severity: {len(c.get("changed", []))}, persisting: {len(c.get("persisting", []))}.', s['body']))
        for label, key in (('New', 'new'), ('Resolved', 'resolved'), ('Changed', 'changed')):
            for item in c.get(key, []):
                extra = f' ({item["from"]} to {item["to"]})' if key == 'changed' else f' ({item["severity"]})'
                els.append(_p(f'- {label}: {_esc(item["title"])}{_esc(extra)}', s['small']))
    els.append(PageBreak())
    return els


def _data_quality(content: ReportContent, s) -> List:
    an, m = content.analysis, content.analysis.metrics
    els: List = [_p('2. Data quality and scope', s['h1'])]
    days = (m.end_time - m.start_time).total_seconds() / 86400
    collected = len(m.metrics)
    attempted = len(m.attempted_metrics)
    els.append(_p(f'Window {m.start_time.strftime("%Y-%m-%d %H:%M")} to {m.end_time.strftime("%Y-%m-%d %H:%M")} UTC ({days:.1f} days), '
                  f'{m.period_seconds // 60}-minute CloudWatch buckets, enhanced monitoring level <b>{_esc(m.monitoring_level)}</b>. '
                  f'{collected} of {attempted} applicable metrics returned data' +
                  (f'; {len(m.not_published)} were not published by the cluster' if m.not_published else '') +
                  (f'; {len(m.collection_errors)} failed to collect' if m.collection_errors else '') +
                  ('. Metric discovery (ListMetrics) was available.' if m.discovery_available else
                   '. Metric discovery (ListMetrics) was not permitted, so unpublished metrics cannot be told apart from empty ones.'),
                  s['body']))
    low = [(n, min(i.coverage_pct for i in items)) for n, items in m.metrics.items() if items and min(i.coverage_pct for i in items) < 90]
    if low:
        els.append(_p('Metrics with less than 90% of the expected datapoints: ' +
                      ', '.join(f'{_esc(n)} ({c:.0f}%)' for n, c in sorted(low)) + '. Gaps usually mean the cluster or the '
                      'metric is younger than the window.', s['body']))
    if m.not_published or m.collection_errors:
        els.append(_p('Metrics not available', s['h2']))
        rows = [['Metric', 'Reason']]
        for n, r in sorted(m.not_published.items()):
            rows.append([_p(_esc(n), s['cell']), _p(_esc(r), s['cell'])])
        for n, errs in sorted(m.collection_errors.items()):
            rows.append([_p(_esc(n), s['cell']), _p(_esc('collection failed: ' + '; '.join(errs[:4])), s['cell'])])
        els.append(_table(rows, [2.0 * inch, 5.0 * inch]))
    na = [f for f in an.findings if f.severity == Severity.NOT_ASSESSED]
    els.append(_p('Checks not assessed', s['h2']))
    if na:
        rows = [['Check', 'Category', 'Reason']]
        for f in na:
            rows.append([_p(_esc(f.title), s['cell']), _p(CATEGORY_TITLES[f.category], s['cell']), _p(_esc(f.reason), s['cell'])])
        els.append(_table(rows, [1.8 * inch, 1.4 * inch, 3.8 * inch]))
        els.append(_p('These checks do not affect the score. Enable the missing metrics or permissions to include them.', s['small']))
    else:
        els.append(_p('Every applicable check was assessed.', s['body']))
    els.append(PageBreak())
    return els


def _action_plan(content: ReportContent, s) -> List:
    els: List = [_p('3. Action plan', s['h1'])]
    recs = content.recommendations
    if not recs:
        els.append(_p('No action required from this review.', s['body']))
        els.append(PageBreak())
        return els
    els.append(_p('Priority reflects urgency; severity is the technical impact of the finding; confidence says how far the '
                  'evidence supports the conclusion. Details and verification steps are in section 4.', s['body']))
    els.append(Spacer(1, 6))
    rows = [['Priority', 'Severity', 'Conf.', 'Finding', 'Action', 'Brokers']]
    fills = {}
    for i, r in enumerate(recs, start=1):
        f = r.finding
        rows.append([_p(_esc(r.priority_label.split(' - ')[0]), s['cellb']), _p(_sev(f.severity), s['cell']),
                     _p(_esc(f.confidence), s['cell']), _p(_esc(f.title), s['cell']), _p(_esc(r.action), s['cell']),
                     _p(_esc(', '.join(r.affected_brokers) or '-'), s['cell'])])
        fills[i] = SEVERITY_FILL[f.severity]
    els.append(_table(rows, [0.55 * inch, 0.8 * inch, 0.55 * inch, 1.8 * inch, 2.7 * inch, 0.6 * inch], zebra=False, row_fills=fills))
    els.append(PageBreak())
    return els


def _stats_table(finding: Finding, metric_list: List[MetricData], s) -> Optional[Table]:
    if not metric_list:
        return None
    unit = metric_list[0].unit or ''
    rows = [['Series', 'Avg', 'P95', 'Max (bucket)', 'Peak (1-min)', 'Last', 'Coverage']]
    for m in metric_list:
        label = f'Broker {m.broker_id}' if m.broker_id else 'Cluster'
        st = m.statistics
        rows.append([label, _fmt_value(st.get('avg'), unit), _fmt_value(st.get('p95'), unit), _fmt_value(st.get('max'), unit),
                     _fmt_value(st.get('peak', st.get('max')), unit), _fmt_value(st.get('last'), unit), f'{m.coverage_pct:.0f}%'])
    basis = metric_list[0].effective_stat
    note = {'SumPerMinute': 'values are broker totals per minute (Sum of network-processor samples / minutes)',
            'Maximum': 'values are the Maximum statistic per bucket', 'Minimum': 'values are the Minimum statistic per bucket'}.get(basis, 'values are averages per bucket')
    t = _table(rows, [1.0 * inch, 0.9 * inch, 0.9 * inch, 1.0 * inch, 1.0 * inch, 0.9 * inch, 0.8 * inch])
    return KeepTogether([t, _p(f'Unit {unit or "count"}; {note}. Peak (1-min) is the highest 1-minute sample in the window.', s['caption'])])


def _chart_flowables(chart, s) -> List:
    if not chart:
        return []
    ratio = chart.height / chart.width if chart.width else 0.5
    width = 6.8 * inch
    img = Image(BytesIO(chart.image_data), width=width, height=width * ratio)
    els = [img]
    if getattr(chart, 'caption', ''):
        els.append(_p(_esc(chart.caption), s['caption']))
    return els


def _finding_block(f: Finding, rec: Optional[Recommendation], metric_list: List[MetricData], chart, s) -> List:
    els: List = [_p(_esc(f.title), s['h2f'])]
    meta = f'{_sev(f.severity)} &nbsp; observed <b>{_esc(f.observed or "-")}</b>'
    if f.threshold:
        meta += f' &nbsp; threshold <b>{_esc(f.threshold)}</b>'
    meta += f' &nbsp; confidence {_esc(f.confidence)}'
    if f.affected_brokers:
        meta += f' &nbsp; brokers {_esc(", ".join(f.affected_brokers))}'
    if f.source_url:
        meta += f' &nbsp; <a href="{f.source_url}" color="#0b5ed7">source</a>'
    els.append(_p(meta, s['body']))
    els.append(Spacer(1, 4))
    els.append(_p(_esc(f.description), s['body']))
    els.append(Spacer(1, 6))
    els.extend(_chart_flowables(chart, s))
    st = _stats_table(f, metric_list, s)
    if st:
        els.append(Spacer(1, 4))
        els.append(st)
    if rec:
        els.append(Spacer(1, 6))
        block = [_p('Recommendation', s['h3']), _p(f'<b>{_esc(rec.priority_label)}.</b> {_esc(rec.action)}', s['body'])]
        if rec.confirm:
            block.append(_p(f'<b>Confirm first:</b> {_esc(rec.confirm)}', s['body']))
        if rec.verify:
            block.append(_p(f'<b>Verify afterwards:</b> {_esc(rec.verify)}', s['body']))
        block.append(_p(f'<b>If not addressed:</b> {_esc(rec.impact)}', s['body']))
        if rec.documentation_links:
            links = ' &nbsp; '.join(f'<a href="{u}" color="#0b5ed7">{_esc(u.split("/")[-1] or u)}</a>' for u in rec.documentation_links)
            block.append(_p(f'<b>Documentation:</b> {links}', s['small']))
        els.append(KeepTogether(block))
    els.append(Spacer(1, 14))
    return els


def _findings_sections(content: ReportContent, s) -> List:
    an = content.analysis
    charts = {c.metric_name: c for c in content.charts}
    recs = {r.finding.check_id: r for r in content.recommendations}
    els: List = [_p('4. Findings that need attention', s['h1'])]
    attention = [f for f in an.findings if f.severity in (Severity.CRITICAL, Severity.HIGH, Severity.WARNING, Severity.INFORMATIONAL)]
    if not attention:
        els.append(_p('No finding requires attention.', s['body']))
    for cat in CATEGORY_ORDER:
        fs = [f for f in attention if f.category == cat]
        if not fs:
            continue
        els.append(_p(f'4.{CATEGORY_ORDER.index(cat) + 1} {CATEGORY_TITLES[cat]}', s['h2']))
        for f in fs:
            metric_list = an.metrics.metrics.get(f.metric_name, []) if f.section == 'metric' else []
            chart = charts.get(f.chart_metric or '')
            els.extend(_finding_block(f, recs.get(f.check_id), metric_list, chart, s))
    els.append(PageBreak())
    return els


def _healthy_section(content: ReportContent, s) -> List:
    an = content.analysis
    charts = {c.metric_name: c for c in content.charts}
    els: List = [_p('5. Healthy checks', s['h1'])]
    healthy = [f for f in an.findings if f.severity == Severity.HEALTHY]
    if not healthy:
        els.append(_p('No check passed without remarks.', s['body']))
        els.append(PageBreak())
        return els
    rows = [['Category', 'Check', 'Observed', 'Threshold']]
    for f in healthy:
        rows.append([_p(CATEGORY_TITLES[f.category], s['cell']), _p(_esc(f.title), s['cell']),
                     _p(_esc(f.observed or '-'), s['cell']), _p(_esc(f.threshold or '-'), s['cell'])])
    els.append(_table(rows, [1.4 * inch, 2.7 * inch, 1.6 * inch, 1.3 * inch]))
    els.append(Spacer(1, 10))
    shown = {f.chart_metric for f in an.findings if f.severity != Severity.HEALTHY and f.chart_metric}
    remaining = [c for name, c in charts.items() if name not in shown]
    if remaining:
        els.append(_p('Metric charts for the healthy checks', s['h2']))
        for chart in remaining:
            els.append(KeepTogether([_p(_esc(chart.title), s['h3'])] + _chart_flowables(chart, s)))
            els.append(Spacer(1, 8))
    els.append(PageBreak())
    return els


def _inventory(content: ReportContent, s) -> List:
    ci = content.cluster_info
    redact = content.redact
    rows = [
        ['Cluster name', _display_name(content)],
        ['Cluster ARN', redact_arn(ci.arn) if redact else ci.arn],
        ['Account / Region', ('************' if redact else ci.account_id) + f' / {ci.region}'],
        ['Broker type', f'{ci.cluster_type.title()} ({ci.instance_type}, {ci.instance_family})'],
        ['Brokers / AZs', f'{ci.broker_count} brokers across {ci.availability_zones or "?"} AZs'],
        ['Kafka version', f'{ci.kafka_version} ({ci.kafka_version_status}), metadata {ci.metadata_mode}'],
        ['State / created', f'{ci.cluster_state}' + (f', {ci.creation_time.strftime("%Y-%m-%d")}' if ci.creation_time else '')],
        ['Authentication', ', '.join(ci.authentication_methods) or 'none reported'],
        ['Encryption', f'client-broker {ci.encryption_in_transit_type}, in-cluster {ci.in_cluster_encryption}, at rest KMS'],
        ['Public access', ci.public_access],
        ['Enhanced monitoring', ci.enhanced_monitoring_level],
        ['Broker logs', ', '.join(ci.logging_destinations) or 'disabled'],
    ]
    if ci.is_express:
        rows.append(['Intelligent rebalancing', ci.rebalancing_status or 'not reported by the API'])
    else:
        rows += [['EBS volume per broker', f'{ci.ebs_volume_size} GiB, storage mode {ci.storage_mode}'],
                 ['Storage auto scaling', ci.storage_autoscaling_detail],
                 ['Provisioned storage throughput', f'{ci.provisioned_throughput_mibps} MiB/s' if ci.provisioned_throughput_enabled else 'disabled']]
    table = _table([[_p(_esc(a), s['cellb']), _p(_esc(b), s['cell'])] for a, b in rows], [2.0 * inch, 5.0 * inch], header=False)
    return [_p('6. Cluster inventory', s['h1']), table, PageBreak()]


def _methodology(content: ReportContent, s) -> List:
    els: List = [_p('7. Methodology and scoring', s['h1'])]
    els.append(_p('Each category starts at 100 and is multiplied by 0.60 for every critical finding, 0.70 for every high '
                  'finding and 0.85 for every warning; informational findings and checks that were not assessed leave the '
                  'score unchanged. The overall score is the weighted average (Reliability 35%, Performance 30%, Security 20%, '
                  'Cost 15%). The status label is bounded by the worst finding: Critical when any critical finding exists '
                  '(operational impact now or imminent), Needs Attention when any high or warning finding exists, Healthy '
                  'otherwise. High is used for posture and resilience gaps - an unauthenticated listener, plaintext client '
                  'traffic, a single availability zone - which may be deliberate choices and do not by themselves make the '
                  'cluster unhealthy.', s['body']))
    els.append(Spacer(1, 6))
    els.append(_p('Statistics. Utilisation gauges (CPU, heap, disk, bytes per second) use the average per bucket (bucket size shown '
                  'in section 2); P95 is the 95th percentile of those values and "peak" is the highest 1-minute sample. CPU User and CPU System are '
                  'summed on matching timestamps before percentiles are computed. Event counters (offline partitions, '
                  'partitions below min ISR, under-replicated partitions) use the Maximum per bucket so short events are not '
                  'averaged away; the controller count uses the Minimum. Connection metrics are published as one '
                  'sample per network processor per minute, so broker totals are the Sum divided by the minutes in the bucket.',
                  s['body']))
    els.append(Spacer(1, 6))
    els.append(_p('Thresholds and their sources', s['h2']))
    rows = [['Check', 'Threshold', 'Source']]
    thr = [
        ('CPU User + System', '< 60% (P95 of hourly averages)', 'MSK best practices'),
        ('HeapMemoryAfterGC', '< 60%', 'MSK best practices'),
        ('KafkaDataLogsDiskUsed', 'act at 85% (warning from 75%, tool guideline)', 'MSK best practices'),
        ('Partitions per broker', 'recommended / maximum per broker size', 'MSK best practices and quotas'),
        ('Express throughput', 'sustained limit / throttle quota per broker size', 'MSK quotas'),
        ('Standard throughput', 'per-size guideline shipped with the tool (no published quota)', 'tool guideline'),
        ('IAM client connections', '3000 per broker', 'MSK quotas'),
        ('IAM connection rate', '100 per second per broker (4 on kafka.t3.small)', 'MSK quotas'),
        ('Broker imbalance', 'hottest broker within 10-25% of the mean, above an activity floor', 'tool guideline'),
        ('Availability zones', '3 for production', 'MSK best practices'),
    ]
    for a, b, c in thr:
        rows.append([_p(a, s['cell']), _p(b, s['cell']), _p(c, s['cell'])])
    els.append(_table(rows, [1.9 * inch, 3.3 * inch, 1.8 * inch]))
    els.append(Spacer(1, 6))
    els.append(_p(f'Workload profile used for severity of resilience checks: <b>{_esc(content.analysis.workload)}</b>. '
                  f'Rules version {ref.RULES_VERSION}; tool version {__version__}.' +
                  (f' Machine-readable manifest: {_esc(content.manifest_filename)}.' if content.manifest_filename else ''), s['body']))
    els.append(Spacer(1, 6))
    els.append(_p('Limitations. Bucket averages can hide short patterns other than the recorded peak; Standard broker '
                  'network guidelines are not AWS quotas; topic-level and client-side behaviour is outside the scope of '
                  'CloudWatch cluster metrics.', s['small']))
    els.append(PageBreak())
    return els


def _references(s) -> List:
    els: List = [_p('8. References', s['h1'])]
    labels = [('best_practices', 'Best practices for Standard brokers'), ('best_practices_express', 'Best practices for Express brokers'),
              ('quotas', 'Amazon MSK quotas'), ('metrics_standard', 'Metrics for Standard brokers'), ('metrics_express', 'Metrics for Express brokers'),
              ('monitoring', 'Monitoring with CloudWatch'), ('storage_autoscaling', 'Automatic storage scaling'), ('kafka_versions', 'Supported Kafka versions'),
              ('encryption', 'Encryption'), ('authentication', 'Authentication and authorisation'), ('logging', 'Broker logs'),
              ('graviton', 'Graviton brokers'), ('broker_sizes', 'Broker sizes'), ('cruise_control', 'Cruise Control on MSK'),
              ('client_best_practices', 'Best practices for Kafka clients')]
    for key, label in labels:
        els.append(_p(f'- <a href="{ref.DOCS[key]}" color="#0b5ed7">{label}</a> <font color="{GREY.hexval()}">{ref.DOCS[key]}</font>', s['small']))
    return els


# --------------------------------------------------------------------------- build

def build_pdf_report(content: ReportContent, output_path: str) -> None:
    """Build the complete PDF report (two passes, so the table of contents has page numbers)."""
    s = _styles()
    footer = (f'Amazon MSK health check - {_display_name(content)} - generated {content.generation_time.strftime("%Y-%m-%d %H:%M")} UTC')
    doc = ReportDocTemplate(output_path, footer_text=footer, pagesize=letter, leftMargin=0.75 * inch, rightMargin=0.75 * inch,
                            topMargin=0.75 * inch, bottomMargin=0.85 * inch,
                            title=f'Amazon MSK health check - {_display_name(content)}', author='msk-health-check')
    story: List = []
    story += _title_page(content, s)
    story += _toc(s)
    story += _executive_summary(content, s)
    story += _data_quality(content, s)
    story += _action_plan(content, s)
    story += _findings_sections(content, s)
    story += _healthy_section(content, s)
    story += _inventory(content, s)
    story += _methodology(content, s)
    story += _references(s)
    doc.multiBuild(story)
