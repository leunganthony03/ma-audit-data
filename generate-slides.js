const PptxGenJS = require("pptxgenjs");

const pptx = new PptxGenJS();

const NAVY   = "1B2A4A";
const BLUE   = "2563EB";
const TEAL   = "0D9488";
const GREEN  = "16A34A";
const ORANGE = "EA580C";
const PURPLE = "7C3AED";
const WHITE  = "FFFFFF";
const GRAY   = "6B7280";
const DARK   = "111827";
const SLATE  = "F8FAFC";

pptx.layout = "LAYOUT_WIDE";

// ─────────────────────────────────────────────
// Slide 1 — Title
// ─────────────────────────────────────────────
{
  const slide = pptx.addSlide();
  slide.background = { color: NAVY };

  // Left accent bar
  slide.addShape(pptx.ShapeType.rect, {
    x: 0, y: 0, w: 0.3, h: 7.5, fill: { color: BLUE },
  });

  // Horizontal rule
  slide.addShape(pptx.ShapeType.rect, {
    x: 0.55, y: 3.4, w: 12.5, h: 0.05, fill: { color: BLUE },
  });

  slide.addText("Engineering Performance", {
    x: 0.55, y: 1.2, w: 11.8, h: 1.0,
    fontSize: 48, bold: true, color: WHITE, fontFace: "Calibri",
  });
  slide.addText("What We Measure & Why It Matters", {
    x: 0.55, y: 2.25, w: 10.0, h: 0.7,
    fontSize: 22, color: "93C5FD", fontFace: "Calibri",
  });

  slide.addText(
    "DORA metrics give engineering leadership a shared language for delivery health — moving beyond output to measure speed, quality, and predictability.",
    { x: 0.55, y: 3.6, w: 11.8, h: 1.1, fontSize: 16, color: "CBD5E1", fontFace: "Calibri" }
  );

  // Four pill labels
  const pills = [
    { label: "PR Time", color: BLUE },
    { label: "Cycle Time", color: TEAL },
    { label: "Planned vs Completed", color: ORANGE },
    { label: "Velocity", color: PURPLE },
  ];
  pills.forEach((p, i) => {
    slide.addShape(pptx.ShapeType.roundRect, {
      x: 0.55 + i * 3.15, y: 6.3, w: 2.95, h: 0.6,
      fill: { color: p.color }, rectRadius: 0.12,
    });
    slide.addText(p.label, {
      x: 0.55 + i * 3.15, y: 6.3, w: 2.95, h: 0.6,
      fontSize: 13, bold: true, color: WHITE, align: "center", fontFace: "Calibri",
    });
  });

  slide.addText("March 2026", {
    x: 0.55, y: 7.1, w: 3, h: 0.3, fontSize: 11, color: "475569", fontFace: "Calibri",
  });
}

// ─────────────────────────────────────────────
// Slide 2 — DORA Framework (1 per quadrant)
// ─────────────────────────────────────────────
{
  const slide = pptx.addSlide();
  slide.background = { color: SLATE };

  slide.addShape(pptx.ShapeType.rect, {
    x: 0, y: 0, w: 13.33, h: 1.05, fill: { color: NAVY },
  });
  slide.addText("The DORA Framework", {
    x: 0.4, y: 0.12, w: 9, h: 0.75,
    fontSize: 26, bold: true, color: WHITE, fontFace: "Calibri",
  });
  slide.addText("Industry-standard measures of software delivery performance", {
    x: 0.4, y: 0.55, w: 9, h: 0.4,
    fontSize: 13, color: "93C5FD", fontFace: "Calibri",
  });

  // 2×2 grid of metric cards
  const cards = [
    {
      title: "Deployment Frequency",
      question: "How often do we ship?",
      elite: "Multiple times per day",
      icon: "🚀",
      color: GREEN,
      x: 0.35, y: 1.2,
    },
    {
      title: "Lead Time for Changes",
      question: "How fast from code to customer?",
      elite: "< 1 hour",
      icon: "⏱",
      color: BLUE,
      x: 6.85, y: 1.2,
    },
    {
      title: "Change Failure Rate",
      question: "How often do releases cause issues?",
      elite: "< 15% of deployments",
      icon: "🔥",
      color: ORANGE,
      x: 0.35, y: 4.2,
    },
    {
      title: "Time to Restore",
      question: "How quickly do we recover?",
      elite: "< 1 hour",
      icon: "⚡",
      color: PURPLE,
      x: 6.85, y: 4.2,
    },
  ];

  cards.forEach((c) => {
    slide.addShape(pptx.ShapeType.roundRect, {
      x: c.x, y: c.y, w: 6.15, h: 2.8,
      fill: { color: WHITE }, line: { color: "E2E8F0", pt: 1 }, rectRadius: 0.14,
    });
    // Left color stripe
    slide.addShape(pptx.ShapeType.rect, {
      x: c.x, y: c.y, w: 0.18, h: 2.8, fill: { color: c.color },
    });
    slide.addText(c.icon, {
      x: c.x + 0.3, y: c.y + 0.25, w: 0.7, h: 0.7, fontSize: 28,
    });
    slide.addText(c.title, {
      x: c.x + 1.1, y: c.y + 0.3, w: 4.8, h: 0.5,
      fontSize: 16, bold: true, color: DARK, fontFace: "Calibri",
    });
    slide.addText(c.question, {
      x: c.x + 0.35, y: c.y + 0.95, w: 5.6, h: 0.5,
      fontSize: 13, color: GRAY, italic: true, fontFace: "Calibri",
    });
    // Divider
    slide.addShape(pptx.ShapeType.rect, {
      x: c.x + 0.35, y: c.y + 1.55, w: 5.55, h: 0.03, fill: { color: "E2E8F0" },
    });
    slide.addText("Elite benchmark:", {
      x: c.x + 0.35, y: c.y + 1.7, w: 1.8, h: 0.4,
      fontSize: 11, bold: true, color: GRAY, fontFace: "Calibri",
    });
    slide.addText(c.elite, {
      x: c.x + 2.1, y: c.y + 1.7, w: 3.8, h: 0.4,
      fontSize: 12, bold: true, color: c.color, fontFace: "Calibri",
    });
    slide.addText("Source: Google DORA State of DevOps Report", {
      x: c.x + 0.35, y: c.y + 2.25, w: 5.5, h: 0.35,
      fontSize: 9, color: "94A3B8", italic: true, fontFace: "Calibri",
    });
  });
}

// ─────────────────────────────────────────────
// Slide 3 — Our Metrics Scorecard
// ─────────────────────────────────────────────
{
  const slide = pptx.addSlide();
  slide.background = { color: SLATE };

  slide.addShape(pptx.ShapeType.rect, {
    x: 0, y: 0, w: 13.33, h: 1.05, fill: { color: NAVY },
  });
  slide.addText("Our Metrics — At a Glance", {
    x: 0.4, y: 0.12, w: 9, h: 0.75,
    fontSize: 26, bold: true, color: WHITE, fontFace: "Calibri",
  });
  slide.addText("Current sprint · March 2026", {
    x: 0.4, y: 0.55, w: 6, h: 0.4,
    fontSize: 13, color: "93C5FD", fontFace: "Calibri",
  });

  const metrics = [
    {
      title: "PR Time",
      value: "18 hrs",
      status: "On Track",
      statusColor: GREEN,
      target: "Target: < 24 hrs",
      what: "Avg time from PR opened → merged",
      why: "Slow PRs block teammates and inflate Lead Time. Keeping this under 24 hrs maintains developer flow.",
      color: BLUE,
      x: 0.35,
    },
    {
      title: "Cycle Time",
      value: "4.2 days",
      status: "On Track",
      statusColor: GREEN,
      target: "Target: < 7 days",
      what: "First commit → production",
      why: "Shorter cycles mean faster learning and lower risk. We're in the 'High' DORA tier; elite is < 1 day.",
      color: TEAL,
      x: 3.6,
    },
    {
      title: "Planned vs\nCompleted",
      value: "87%",
      status: "Watch",
      statusColor: ORANGE,
      target: "Target: ≥ 90%",
      what: "Sprint commitment hit rate",
      why: "13% of committed work didn't ship. Common causes: scope creep and underestimation. Addressing in next retro.",
      color: ORANGE,
      x: 6.85,
    },
    {
      title: "Velocity",
      value: "31 pts",
      status: "Improving",
      statusColor: GREEN,
      target: "3-sprint avg: 29 pts",
      what: "Story points completed / sprint",
      why: "Trending up 7%. Used for forecasting only — not a performance target for individuals.",
      color: PURPLE,
      x: 10.1,
    },
  ];

  metrics.forEach((m) => {
    // Card
    slide.addShape(pptx.ShapeType.roundRect, {
      x: m.x, y: 1.2, w: 3.0, h: 6.0,
      fill: { color: WHITE }, line: { color: "E2E8F0", pt: 1 }, rectRadius: 0.14,
    });
    // Top color bar
    slide.addShape(pptx.ShapeType.rect, {
      x: m.x, y: 1.2, w: 3.0, h: 0.22, fill: { color: m.color },
    });

    // Metric title
    slide.addText(m.title, {
      x: m.x + 0.15, y: 1.5, w: 2.7, h: 0.55,
      fontSize: 14, bold: true, color: DARK, align: "center", fontFace: "Calibri",
    });

    // Big value
    slide.addText(m.value, {
      x: m.x + 0.1, y: 2.1, w: 2.8, h: 0.85,
      fontSize: 34, bold: true, color: m.color, align: "center", fontFace: "Calibri",
    });

    // Status badge
    slide.addShape(pptx.ShapeType.roundRect, {
      x: m.x + 0.5, y: 3.05, w: 2.0, h: 0.4,
      fill: { color: m.statusColor }, rectRadius: 0.1,
    });
    slide.addText(m.status, {
      x: m.x + 0.5, y: 3.05, w: 2.0, h: 0.4,
      fontSize: 11, bold: true, color: WHITE, align: "center", fontFace: "Calibri",
    });

    // Target
    slide.addText(m.target, {
      x: m.x + 0.1, y: 3.55, w: 2.8, h: 0.35,
      fontSize: 10, color: GRAY, align: "center", fontFace: "Calibri",
    });

    // Divider
    slide.addShape(pptx.ShapeType.rect, {
      x: m.x + 0.15, y: 4.0, w: 2.7, h: 0.03, fill: { color: "E2E8F0" },
    });

    // What label
    slide.addText("WHAT", {
      x: m.x + 0.15, y: 4.1, w: 2.7, h: 0.28,
      fontSize: 8, bold: true, color: GRAY, fontFace: "Calibri",
    });
    slide.addText(m.what, {
      x: m.x + 0.15, y: 4.38, w: 2.7, h: 0.5,
      fontSize: 10, color: DARK, fontFace: "Calibri",
    });

    // Divider
    slide.addShape(pptx.ShapeType.rect, {
      x: m.x + 0.15, y: 4.95, w: 2.7, h: 0.03, fill: { color: "E2E8F0" },
    });

    // Why label
    slide.addText("SO WHAT", {
      x: m.x + 0.15, y: 5.05, w: 2.7, h: 0.28,
      fontSize: 8, bold: true, color: GRAY, fontFace: "Calibri",
    });
    slide.addText(m.why, {
      x: m.x + 0.15, y: 5.33, w: 2.7, h: 1.7,
      fontSize: 10, color: DARK, fontFace: "Calibri",
    });
  });
}

// ─────────────────────────────────────────────
// Slide 4 — Executive Summary & Actions
// ─────────────────────────────────────────────
{
  const slide = pptx.addSlide();
  slide.background = { color: NAVY };

  slide.addShape(pptx.ShapeType.rect, {
    x: 0, y: 0, w: 13.33, h: 1.05, fill: { color: "0F172A" },
  });
  slide.addText("What This Means & What's Next", {
    x: 0.4, y: 0.12, w: 12, h: 0.75,
    fontSize: 26, bold: true, color: WHITE, fontFace: "Calibri",
  });

  // Three columns: Strengths / Watch / Actions
  const cols = [
    {
      heading: "Strengths",
      icon: "✅",
      color: GREEN,
      items: [
        "PR merge time (18 hrs) within healthy range — team review culture is working",
        "Cycle time (4.2 days) places us in the top DORA quartile for our industry",
        "Velocity trending up 7% over last 3 sprints without added headcount",
      ],
      x: 0.35,
    },
    {
      heading: "Watch Items",
      icon: "⚠️",
      color: ORANGE,
      items: [
        "Sprint completion at 87% — below our 90% target for 2 consecutive sprints",
        "Root cause: unplanned incident work consuming ~1 sprint-day per engineer",
        "Cycle time still 4× away from elite benchmark — opportunity for automation",
      ],
      x: 4.75,
    },
    {
      heading: "Next Actions",
      icon: "🎯",
      color: BLUE,
      items: [
        "Reserve 10% sprint capacity as buffer for unplanned work (starting Sprint 12)",
        "Pilot automated deployment pipeline to reduce manual release steps",
        "Set team PR review SLA of 4 hrs — track via weekly dashboard",
      ],
      x: 9.15,
    },
  ];

  cols.forEach((col) => {
    slide.addShape(pptx.ShapeType.roundRect, {
      x: col.x, y: 1.2, w: 4.05, h: 4.8,
      fill: { color: "1E293B" }, line: { color: col.color, pt: 2 }, rectRadius: 0.14,
    });
    // Heading stripe
    slide.addShape(pptx.ShapeType.rect, {
      x: col.x, y: 1.2, w: 4.05, h: 0.18, fill: { color: col.color },
    });
    slide.addText(`${col.icon}  ${col.heading}`, {
      x: col.x + 0.15, y: 1.42, w: 3.75, h: 0.55,
      fontSize: 15, bold: true, color: WHITE, fontFace: "Calibri",
    });
    col.items.forEach((item, i) => {
      // Bullet dot
      slide.addShape(pptx.ShapeType.ellipse, {
        x: col.x + 0.18, y: 2.12 + i * 1.35, w: 0.12, h: 0.12,
        fill: { color: col.color },
      });
      slide.addText(item, {
        x: col.x + 0.38, y: 2.07 + i * 1.35, w: 3.5, h: 1.2,
        fontSize: 11, color: "E2E8F0", fontFace: "Calibri",
      });
    });
  });

  // Bottom banner — one-liner
  slide.addShape(pptx.ShapeType.roundRect, {
    x: 0.35, y: 6.2, w: 12.6, h: 1.05,
    fill: { color: "1E293B" }, line: { color: "334155", pt: 1 }, rectRadius: 0.1,
  });
  slide.addText(
    "Bottom line:  We ship reliably and are improving. One focused action — protecting sprint capacity — will close the planning gap and push all four metrics into the elite tier.",
    {
      x: 0.55, y: 6.28, w: 12.2, h: 0.9,
      fontSize: 13, bold: false, color: "E2E8F0", fontFace: "Calibri",
    }
  );
}

pptx.writeFile({ fileName: "DORA_Metrics.pptx" })
  .then(() => console.log("✅  DORA_Metrics.pptx created successfully!"))
  .catch(err => console.error("Error:", err));
