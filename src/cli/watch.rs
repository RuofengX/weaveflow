use chrono::Utc;
use std::collections::{HashMap, HashSet};
use std::io;

use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{prelude::*, widgets::*};
use serde_json::Value;
use tokio::sync::mpsc;

/// Run ratatui TUI mode.
pub fn run_tui(
    rx: &mut mpsc::UnboundedReceiver<Value>,
    task_id: &str,
    pipeline_name: &str,
) -> io::Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut state = TuiState {
        task_id: task_id.to_string(),
        pipeline_name: pipeline_name.to_string(),
        data: None,
        done: false,
        error: None,
    };

    let res = run_app(&mut terminal, &mut state, rx);

    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    match res {
        Ok(()) => {
            if let Some(ref data) = state.data {
                let out = data
                    .get("status")
                    .and_then(|s| s.get("Completed"));
                if let Some(out) = out {
                    if out.get("_binary").and_then(|v| v.as_bool()).unwrap_or(false) {
                        let size = out.get("_size").and_then(|v| v.as_u64()).unwrap_or(0);
                        println!("\n[binary {size} bytes]");
                    } else if out.is_object() || out.is_array() {
                        println!(
                            "\n{}",
                            serde_json::to_string_pretty(out).unwrap_or_default()
                        );
                    } else if let Some(s) = out.as_str() {
                        if s.len() > 200 {
                            println!("\n{}... ({} chars)", &s[..200], s.len());
                        } else {
                            println!("\n{s}");
                        }
                    }
                }
                if let Some(dur) = data.get("total_duration_ms").and_then(|v| v.as_u64()) {
                    println!("Completed in {}ms", dur);
                }
            }
            if let Some(ref err) = state.error {
                eprintln!("Error: {err}");
            }
        }
        Err(e) => {
            eprintln!("TUI error: {e}");
        }
    }

    Ok(())
}

/// Run text output mode (for CI/CD, agents). Prints one line per layer on completion.
pub async fn run_text(rx: &mut mpsc::UnboundedReceiver<Value>) {
    let mut completed_layers: HashSet<usize> = HashSet::new();
    loop {
        match rx.recv().await {
            Some(data) => {
                let status = data
                    .get("status")
                    .and_then(|s| s.as_object())
                    .and_then(|o| o.keys().next().map(|k| k.as_str()))
                    .unwrap_or("unknown");

                print_text_layer(&data, &mut completed_layers);

                if status == "Completed" || status == "Failed" {
                    if status == "Completed" {
                        if let Some(out) = data
                            .get("status")
                            .and_then(|s| s.get("Completed"))
                        {
                            if out.get("_binary").and_then(|v| v.as_bool()).unwrap_or(false) {
                                let size = out.get("_size").and_then(|v| v.as_u64()).unwrap_or(0);
                                println!("[weave] output: [binary {size} bytes]");
                            } else if out.is_string() && out.as_str().map(|s| s.len()).unwrap_or(0) > 200 {
                                let s = out.as_str().unwrap();
                                println!("[weave] output: {}... ({} chars)", &s[..200], s.len());
                            } else {
                                println!(
                                    "[weave] output: {}",
                                    serde_json::to_string(out).unwrap_or_default()
                                );
                            }
                        }
                        if let Some(steps) = data.get("steps").and_then(|s| s.as_array()) {
                            println!("[weave] step progress:");
                            for step in steps {
                                let sid = step.get("step_id").and_then(|v| v.as_str()).unwrap_or("?");
                                let state = step.get("state").and_then(|s| s.as_object());
                                let state_str = match state {
                                    Some(obj) => {
                                        let variant = obj.keys().next().map(|k| k.as_str()).unwrap_or("?");
                                        match variant {
                                            "Completed" => {
                                                let dur = obj.get("Completed")
                                                    .and_then(|c| c.get("duration_ms"))
                                                    .and_then(|v| v.as_u64())
                                                    .unwrap_or(0);
                                                let cached = obj.get("Completed")
                                                    .and_then(|c| c.get("cached"))
                                                    .and_then(|v| v.as_bool())
                                                    .unwrap_or(false);
                                                if cached { format!("✓ ({dur}ms ♻)") } else { format!("✓ ({dur}ms)") }
                                            }
                                            "Failed" => {
                                                let err = obj.get("Failed")
                                                    .and_then(|f| f.get("error"))
                                                    .and_then(|v| v.as_str())
                                                    .unwrap_or("?");
                                                format!("✗ ({err})")
                                            }
                                            "Iterating" => "◐".to_string(),
                                            "Running" => "●".to_string(),
                                            _ => variant.to_string(),
                                        }
                                    }
                                    None => "?".to_string(),
                                };
                                println!("  {sid}: {state_str}");
                            }
                        }
                    }
                    if status == "Failed" {
                        let err = data
                            .get("status")
                            .and_then(|s| s.get("Failed"))
                            .and_then(|f| f.as_str())
                            .unwrap_or("unknown error");
                        eprintln!("[weave] error: {err}");
                    }
                    if let Some(dur) = data.get("total_duration_ms").and_then(|v| v.as_u64()) {
                        println!("[weave] completed in {}ms", dur);
                    }
                    break;
                }
            }
            None => break,
        }
    }
}

/// Print layer completion lines. Each layer is printed exactly once when all
/// its steps reach a terminal state (Completed or Failed).
fn print_text_layer(data: &Value, completed: &mut HashSet<usize>) {
    let layers = data
        .get("layers")
        .and_then(|l| l.as_array())
        .map(|a| a.to_vec())
        .unwrap_or_default();

    let step_details = steps_detail_map(data);

    for layer in &layers {
        let idx = layer
            .get("index")
            .and_then(|i| i.as_u64())
            .unwrap_or(0) as usize;
        if completed.contains(&idx) {
            continue;
        }

        let step_ids: Vec<&str> = layer
            .get("step_ids")
            .and_then(|s| s.as_array())
            .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
            .unwrap_or_default();

        let all_terminal = step_ids.iter().all(|sid| {
            matches!(
                step_details.get(*sid).map(|(v, _)| v.as_str()),
                Some("Completed") | Some("Failed")
            )
        });

        if !all_terminal {
            continue;
        }

        completed.insert(idx);

        let parts: Vec<String> = step_ids
            .iter()
            .map(|sid| {
                let (variant, detail) = step_details
                    .get(*sid)
                    .map(|(v, d)| (v.as_str(), d.as_str()))
                    .unwrap_or(("?", "?"));
                let icon = match variant {
                    "Completed" => "✓",
                    "Failed" => "✗",
                    _ => "?",
                };
                format!("{sid} {icon} ({detail})")
            })
            .collect();

        let parallel = if step_ids.len() > 1 { " {}" } else { "" };
        println!(
            "[weave] Layer {}{}: {}",
            idx + 1,
            parallel,
            parts.join(", ")
        );
    }
}

/// Extract step_id → (state_variant, detail_string) from snapshot Value.
fn steps_detail_map(data: &Value) -> HashMap<String, (String, String)> {
    let mut map = HashMap::new();
    let Some(status_obj) = data.get("status").and_then(|s| s.as_object()) else {
        return map;
    };

    for (_variant, inner) in status_obj {
        if let Some(steps_arr) = inner.get("steps").and_then(|s| s.as_array()) {
            for step in steps_arr {
                let step_id = step
                    .get("step_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("?")
                    .to_string();
                let state_obj = step.get("state").and_then(|s| s.as_object());

                let (variant, detail) = match state_obj {
                    Some(obj) => {
                        let variant = obj
                            .keys()
                            .next()
                            .map(|k| k.as_str())
                            .unwrap_or("?")
                            .to_string();
                        let detail = match variant.as_str() {
                            "Completed" => {
                                let dur = obj
                                    .get("Completed")
                                    .and_then(|c| c.get("duration_ms"))
                                    .and_then(|v| v.as_u64())
                                    .unwrap_or(0);
                                format!("{dur}ms")
                            }
                            "Failed" => obj
                                .get("Failed")
                                .and_then(|f| f.get("error"))
                                .and_then(|v| v.as_str())
                                .unwrap_or("error")
                                .to_string(),
                            _ => variant.clone(),
                        };
                        (variant, detail)
                    }
                    None => ("?".to_string(), "?".to_string()),
                };

                map.insert(step_id, (variant, detail));
            }
        }
    }
    map
}

// ── TUI State ────────────────────────────────────────────────────────────

struct TuiState {
    task_id: String,
    pipeline_name: String,
    data: Option<Value>,
    done: bool,
    error: Option<String>,
}

fn run_app(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    state: &mut TuiState,
    rx: &mut mpsc::UnboundedReceiver<Value>,
) -> io::Result<()> {
    loop {
        if let Ok(data) = rx.try_recv() {
            let status = data
                .get("status")
                .and_then(|s| s.as_object())
                .and_then(|o| o.keys().next().map(|k| k.as_str()))
                .unwrap_or("unknown");

            if status == "Failed" {
                state.error = data
                    .get("status")
                    .and_then(|s| s.get("Failed"))
                    .and_then(|f| f.as_str())
                    .map(|s| s.to_string());
            }
            if status == "Completed" || status == "Failed" {
                state.done = true;
            }
            state.data = Some(data);
        }

        terminal.draw(|f| ui(f, state))?;

        if state.done {
            std::thread::sleep(std::time::Duration::from_millis(800));
            return Ok(());
        }

        if event::poll(std::time::Duration::from_millis(100))?
            && let Event::Key(key) = event::read()?
                && key.kind == KeyEventKind::Press
                    && matches!(key.code, KeyCode::Char('q') | KeyCode::Esc) {
                        return Ok(());
                    }

        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}

fn ui(f: &mut Frame, state: &TuiState) {
    let area = f.area();
    let title = format!(
        " weave run: {} — {} ",
        state.pipeline_name, state.task_id
    );
    let block = Block::default()
        .borders(Borders::ALL)
        .title(title)
        .border_style(Style::default().fg(Color::Cyan));
    let inner = block.inner(area);
    f.render_widget(block, area);

    let Some(ref data) = state.data else {
        let msg = Paragraph::new("Waiting for task to start...").alignment(Alignment::Center);
        f.render_widget(msg, inner);
        return;
    };

    let layers = data
        .get("layers")
        .and_then(|l| l.as_array())
        .map(|a| a.to_vec())
        .unwrap_or_default();

    // Build step state map: step_id → (state_variant, detail)
    let step_states = build_step_states(data);

    let mut lines: Vec<Line> = Vec::new();
    let mut counts = Counts::default();

    for layer in &layers {
        let step_ids: Vec<&str> = layer
            .get("step_ids")
            .and_then(|s| s.as_array())
            .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
            .unwrap_or_default();
        let layer_idx = layer.get("index").and_then(|i| i.as_u64()).unwrap_or(0);
        let is_concurrent = step_ids.len() > 1;

        lines.push(Line::from(format!("  Layer {}:", layer_idx + 1)));
        if is_concurrent {
            lines.push(Line::from("  {"));
        }

        for sid in &step_ids {
            let (icon, detail) = match step_states.get(*sid) {
                Some((state_variant, detail_str)) => {
                    match state_variant.as_str() {
                        "Pending" => {
                            counts.pending += 1;
                            ("○", "pending".to_string())
                        }
                        "Running" => {
                            counts.running += 1;
                            ("●", detail_str.clone())
                        }
                        "Iterating" => {
                            counts.iterating += 1;
                            ("◐", detail_str.clone())
                        }
                        "Completed" => {
                            counts.completed += 1;
                            ("✓", detail_str.clone())
                        }
                        "Failed" => {
                            counts.failed += 1;
                            ("✗", detail_str.clone())
                        }
                        _ => {
                            counts.pending += 1;
                            ("○", "?".to_string())
                        }
                    }
                }
                None => {
                    counts.pending += 1;
                    ("○", "unknown".to_string())
                }
            };

            let indent = if is_concurrent { "    " } else { "  " };
            let icon_style = match icon {
                "●" => Style::default().fg(Color::Green).bold(),
                "◐" => Style::default().fg(Color::Cyan),
                "✓" => Style::default().fg(Color::Green),
                "✗" => Style::default().fg(Color::Red),
                _ => Style::default().fg(Color::Yellow),
            };

            lines.push(Line::from(vec![
                Span::raw(indent),
                Span::styled(icon, icon_style),
                Span::raw(format!(" {sid}  ")),
                Span::styled(detail, Style::default().fg(Color::DarkGray)),
            ]));
        }

        if is_concurrent {
            lines.push(Line::from("  }"));
        }
    }

    // Footer
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "══════════════════════════════════════════════",
        Style::default().fg(Color::DarkGray),
    )));
    lines.push(Line::from(format!(
        "● {} running  ◐ {} iterating  ✓ {} done  ○ {} pending  ✗ {} failed",
        counts.running, counts.iterating, counts.completed, counts.pending, counts.failed,
    )));

    let status = data
        .get("status")
        .and_then(|s| s.as_object())
        .and_then(|o| o.keys().next().map(|k| k.as_str()))
        .unwrap_or("?");
    match status {
        "Completed" => lines.push(Line::from(Span::styled("COMPLETED", Style::default().fg(Color::Green).bold()))),
        "Failed" => {
            let err = state.error.as_deref().unwrap_or("unknown");
            lines.push(Line::from(Span::styled(format!("FAILED: {err}"), Style::default().fg(Color::Red).bold())));
        }
        _ => {}
    }

    f.render_widget(Paragraph::new(lines), inner);
}

#[derive(Default)]
struct Counts {
    running: u32,
    iterating: u32,
    completed: u32,
    pending: u32,
    failed: u32,
}

/// Build step_id → (state_variant, detail_string).
fn build_step_states(data: &Value) -> HashMap<String, (String, String)> {
    let mut map = HashMap::new();

    // Prefer top-level "steps" array (always present in TaskSnapshot).
    // Fall back to "status.{variant}.steps" for backward compatibility.
    let steps_arr = data
        .get("steps")
        .and_then(|s| s.as_array())
        .or_else(|| {
            data.get("status")
                .and_then(|s| s.as_object())
                .and_then(|obj| obj.values().next())
                .and_then(|inner| inner.get("steps"))
                .and_then(|s| s.as_array())
        });

    let Some(steps_arr) = steps_arr else {
        return map;
    };

    for step in steps_arr {
        let step_id = step
            .get("step_id")
            .and_then(|v| v.as_str())
            .unwrap_or("?")
            .to_string();
        let state_obj = step.get("state").and_then(|s| s.as_object());

        let (variant, detail) = match state_obj {
            Some(obj) => {
                let variant = obj.keys().next().map(|k| k.as_str()).unwrap_or("?").to_string();
                let detail = match variant.as_str() {
                    "Pending" => "pending".to_string(),
                    "Running" => {
                        let started = obj
                            .get("Running")
                            .and_then(|r| r.get("started_at"))
                            .and_then(|v| v.as_str())
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(Utc::now());
                        let elapsed = (Utc::now() - started).num_milliseconds();
                        format!("running {}ms", elapsed.max(0))
                    }
                    "Iterating" => {
                        let progress = obj
                            .get("Iterating")
                            .and_then(|i| i.get("progress"));
                        let done = progress.and_then(|p| p.get("done")).and_then(|v| v.as_u64()).unwrap_or(0);
                        let total = progress.and_then(|p| p.get("total")).and_then(|v| v.as_u64()).unwrap_or(0);
                        let bar = progress_bar(done, total, 20);
                        format!("{bar} {done}/{total}")
                    }
                    "Completed" => {
                        let dur = obj
                            .get("Completed")
                            .and_then(|c| c.get("duration_ms"))
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0);
                        let cached = obj
                            .get("Completed")
                            .and_then(|c| c.get("cached"))
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false);
                        let cache = if cached { " ♻" } else { "" };
                        format!("{dur}ms{cache}")
                    }
                    "Failed" => {
                        obj.get("Failed")
                            .and_then(|f| f.get("error"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("error")
                            .to_string()
                    }
                    _ => "?".to_string(),
                };
                (variant, detail)
            }
            None => ("?".to_string(), "?".to_string()),
        };

        map.insert(step_id, (variant, detail));
    }
    map
}

fn progress_bar(done: u64, total: u64, width: usize) -> String {
    if total == 0 {
        return "█".repeat(width);
    }
    let filled = ((done as f64 / total as f64) * width as f64) as usize;
    let filled = filled.min(width);
    format!("{}{}", "█".repeat(filled), "░".repeat(width - filled))
}
