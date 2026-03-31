use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use anyhow::Result;
use clap::Parser;
use chrono::Datelike;
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::Client;
use rusqlite::params;
use serde_json::json;
use std::collections::HashMap;
use std::io::Write;

const API_AIRPORTS: &str = "https://api.skycards.oldapes.com/airports";
const API_HIGHSCORE_TEMPLATE: &str = "https://api.skycards.oldapes.com/highscore/airport/";
const API_USER_TEMPLATE: &str = "https://api.skycards.oldapes.com/users/pub/";

const TOKEN: &str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIwMTljMmVmMi0xMDEyLTcwNjEtYWUzNS0wNjlmMThhZTQyYjIiLCJqdGkiOiJIaVdiZlVFM0lnMjBONkhqYnVWT2pfeGFZa2VlNDRaSVZHcFpXX2htSGVNIiwiaWF0IjoxNzcwODI1MzM3LCJleHAiOjQ5MjY1ODUzMzd9.-fOLPEtuvKqOuLnk2EZT8f_Lf-ymMIp4_vjnVgqzNEo";

const BATCH_SIZE: usize = 200;
const DB_PATH: &str = "data/DB/highscore.db";

#[derive(Parser)]
#[command(about = "Fetch Skycards airport leaderboards and user stats")]
struct Args {
    #[arg(long, help = "Specific ISO week to fetch (format YYYYWW)")]
    week: Option<String>,

    #[arg(long, default_value = "3", help = "Number of latest ISO weeks to fetch")]
    last: u32,

    #[arg(long, default_value = "12", help = "Number of concurrent workers")]
    workers: usize,
}

#[derive(serde::Deserialize, Debug)]
struct Airport {
    #[serde(alias = "airportID", alias = "airportId", alias = "airport_id")]
    id: Option<i32>,
    #[serde(default)]
    iata: Option<String>,
    #[serde(default)]
    icao: Option<String>,
}

#[derive(serde::Deserialize, Debug)]
struct LeaderboardResponse {
    highscore: Option<Vec<LeaderboardEntry>>,
    rows: Option<Vec<LeaderboardEntry>>,
    data: Option<Vec<LeaderboardEntry>>,
}

#[derive(serde::Deserialize, Debug)]
struct LeaderboardEntry {
    #[serde(alias = "userId", alias = "id", alias = "playerId")]
    user_id: Option<String>,
    #[serde(alias = "userName", alias = "displayName", alias = "name")]
    user_name: Option<String>,
    #[serde(alias = "userXP", alias = "xp", alias = "score")]
    xp: Option<i32>,
}

#[derive(serde::Deserialize, Debug)]
struct UserProfile {
    #[serde(alias = "userXP", alias = "xp")]
    user_xp: Option<i32>,
    #[serde(alias = "aircraftCount", alias = "aircraft_count", alias = "aircrafts", alias = "aircraftsCount", alias = "numAircraftModels")]
    aircraft_count: Option<i32>,
    #[serde(alias = "destinations", alias = "destinationsCount", alias = "destinationCount", alias = "numDestinations")]
    destinations: Option<i32>,
    #[serde(alias = "battleWins", alias = "battlesWon", alias = "numBattleWins")]
    battle_wins: Option<i32>,
    #[serde(default)]
    achievements: Option<serde_json::Value>,
    #[serde(default)]
    trophies: Option<serde_json::Value>,
}

#[derive(Clone, Debug)]
struct StatsRow {
    user_id: String,
    user_name: Option<String>,
    xp: i32,
    aircraft_count: Option<i32>,
    destinations: Option<i32>,
    battle_wins: Option<i32>,
}

fn get_last_n_weeks(n: u32) -> Vec<String> {
    let today = chrono::Local::now();
    let iso = today.iso_week();
    let cyear = iso.year();
    let cweek = iso.week();
    
    let mut weeks = Vec::new();
    let mut year = cyear;
    let mut week = cweek as i32;

    for i in (0..n as i32).rev() {
        let w = week - i;
        let mut y = year;
        let mut wk = w;

        while wk <= 0 {
            y -= 1;
            let last_week = chrono::NaiveDate::from_ymd_opt(y, 12, 28)
                .and_then(|d| Some(d.iso_week().week()))
                .unwrap_or(52);
            wk += last_week as i32;
        }

        let last_week = chrono::NaiveDate::from_ymd_opt(y, 12, 28)
            .and_then(|d| Some(d.iso_week().week()))
            .unwrap_or(52);
        
        if wk > last_week as i32 {
            wk = last_week as i32;
        }

        weeks.push(format!("{}{:02}", y, wk));
    }
    weeks
}

fn create_client() -> Client {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        reqwest::header::AUTHORIZATION,
        format!("Bearer {}", TOKEN).parse().unwrap(),
    );
    headers.insert(
        reqwest::header::ACCEPT,
        "application/json".parse().unwrap(),
    );
    headers.insert(
        reqwest::header::CONTENT_TYPE,
        "application/json".parse().unwrap(),
    );
    headers.insert(
        "Host",
        "api.skycards.oldapes.com".parse().unwrap(),
    );
    headers.insert(
        "x-client-version",
        "2.0.24".parse().unwrap(),
    );

    reqwest::Client::builder()
        .default_headers(headers)
        .timeout(Duration::from_secs(20))
        .build()
        .unwrap()
}

async fn fetch_airports(client: &Client) -> Result<Vec<Airport>> {
    let resp = client.get(API_AIRPORTS).send().await?;
    let text = resp.text().await?;
    
    if text.is_empty() {
        return Ok(Vec::new());
    }
    
    let data: serde_json::Value = serde_json::from_str(&text)?;

    let airports = match data {
        serde_json::Value::Object(obj) => {
            if let Some(rows) = obj.get("rows") {
                serde_json::from_value(rows.clone())?
            } else if let Some(airports) = obj.get("airports") {
                serde_json::from_value(airports.clone())?
            } else {
                Vec::new()
            }
        }
        serde_json::Value::Array(arr) => serde_json::from_value(serde_json::Value::Array(arr))?,
        _ => Vec::new(),
    };

    Ok(airports)
}

async fn fetch_user_profile(client: &Client, user_id: &str) -> Result<UserProfile> {
    let url = format!("{}{}", API_USER_TEMPLATE, user_id);
    let resp = client.get(&url).timeout(Duration::from_secs(10)).send().await?;
    Ok(resp.json().await?)
}

fn setup_db() -> Result<rusqlite::Connection> {
    let conn = rusqlite::Connection::open(DB_PATH)?;
    
    // Set up WAL mode (use query with no results)
    let _ = conn.query_row("PRAGMA journal_mode=WAL;", [], |_| Ok(()));
    let _ = conn.query_row("PRAGMA busy_timeout = 30000;", [], |_| Ok(()));

    conn.execute(
        "CREATE TABLE IF NOT EXISTS airport_highscore (
            userId TEXT PRIMARY KEY,
            userName TEXT,
            userXP INTEGER,
            aircraftCount INTEGER,
            destinations INTEGER,
            battleWins INTEGER
        )",
        [],
    )?;

    // Add new columns to existing table if they don't exist
    let _ = conn.execute(
        "ALTER TABLE airport_highscore ADD COLUMN aircraftCount INTEGER DEFAULT NULL",
        [],
    );
    let _ = conn.execute(
        "ALTER TABLE airport_highscore ADD COLUMN destinations INTEGER DEFAULT NULL",
        [],
    );
    let _ = conn.execute(
        "ALTER TABLE airport_highscore ADD COLUMN battleWins INTEGER DEFAULT NULL",
        [],
    );

    Ok(conn)
}

async fn writer_task(mut rx: mpsc::Receiver<StatsRow>) -> Result<usize> {
    let mut conn = setup_db()?;
    let mut batch = Vec::new();
    let mut total = 0;

    while let Some(row) = rx.recv().await {
        batch.push(row);
        if batch.len() >= BATCH_SIZE {
            {
                let tx = conn.transaction()?;
                for r in &batch {
                    let user_name = r.user_name.as_deref().unwrap_or("");
                    tx.execute(
                        "INSERT OR REPLACE INTO airport_highscore (userId, userName, userXP, aircraftCount, destinations, battleWins) VALUES (?, ?, ?, ?, ?, ?)",
                        params![&r.user_id, user_name, r.xp, r.aircraft_count, r.destinations, r.battle_wins],
                    )?;
                }
                tx.commit()?;
            }
            total += batch.len();
            batch.clear();
        }
    }

    // Final flush
    if !batch.is_empty() {
        {
            let tx = conn.transaction()?;
            for r in &batch {
                let user_name = r.user_name.as_deref().unwrap_or("");
                tx.execute(
                    "INSERT OR REPLACE INTO airport_highscore (userId, userName, userXP, aircraftCount, destinations, battleWins) VALUES (?, ?, ?, ?, ?, ?)",
                    params![&r.user_id, user_name, r.xp, r.aircraft_count, r.destinations, r.battle_wins],
                )?;
            }
            tx.commit()?;
        }
        total += batch.len();
    }

    Ok(total)
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();

    let weeks = if let Some(week) = args.week {
        vec![week]
    } else {
        get_last_n_weeks(args.last)
    };

    if weeks.is_empty() {
        eprintln!("No weeks to process.");
        return Ok(());
    }

    println!(
        "Fetching {} weeks per airport: {}..{}",
        weeks.len(),
        weeks[0],
        weeks[weeks.len() - 1]
    );

    let client = create_client();
    
    print!("Fetching airports... ");
    std::io::Write::flush(&mut std::io::stdout()).ok();
    let airports = fetch_airports(&client).await?;
    println!("✓ ({} airports)", airports.len());
    
    // Setup database once before starting workers
    setup_db()?;

    let (tx, rx) = mpsc::channel(BATCH_SIZE * 2);
    let writer_handle = tokio::spawn(writer_task(rx));

    let total_weeks = weeks.len() as u64;
    let mut week_num = 0;

    for iso_week in weeks {
        week_num += 1;
        println!(
            "\n[Week {}/{}] {} (processing {} airports)",
            week_num, total_weeks, iso_week, airports.len()
        );

        let bar = ProgressBar::new(airports.len() as u64);
        bar.set_style(
            ProgressStyle::default_bar()
                .template("  {spinner:.green} [{bar:35.cyan/blue}] {pos:>4}/{len:4} airports")
                .unwrap()
                .progress_chars("▓▒░")
                .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
        );

        let mut tasks = Vec::new();
        for airport in &airports {
            if airport.id.is_none() {
                bar.inc(1);
                continue;
            }

            let client = client.clone();
            let iso = iso_week.clone();
            let airport_id = airport.id.unwrap();
            let tx = tx.clone();
            let bar = bar.clone();

            let task = tokio::spawn(async move {
                let url = format!("{}{}", API_HIGHSCORE_TEMPLATE, airport_id);
                let saved = match client
                    .get(&url)
                    .query(&[("isoYearWeek", &iso)])
                    .send()
                    .await
                {
                    Ok(resp) => {
                        if !resp.status().is_success() {
                            0
                        } else {
                            match resp.text().await {
                                Ok(text) => {
                                    if text.is_empty() {
                                        0
                                    } else {
                                        match serde_json::from_str::<LeaderboardResponse>(&text) {
                                            Ok(payload) => {
                                                let mut saved = 0;
                                                let entries = payload
                                                    .highscore
                                                    .or(payload.rows)
                                                    .or(payload.data)
                                                    .unwrap_or_default();

                                                for entry in entries {
                                                    if let Some(uid) = entry.user_id {
                                                        // Fetch full user profile for all stats
                                                        let (xp_val, ac, dc, bw) = match fetch_user_profile(&client, &uid).await {
                                                            Ok(profile) => (
                                                                profile.user_xp,
                                                                profile.aircraft_count,
                                                                profile.destinations,
                                                                profile.battle_wins,
                                                            ),
                                                            Err(_) => (None, None, None, None),
                                                        };

                                                        let row = StatsRow {
                                                            user_id: uid,
                                                            user_name: entry.user_name,
                                                            xp: xp_val.unwrap_or(0),
                                                            aircraft_count: ac,
                                                            destinations: dc,
                                                            battle_wins: bw,
                                                        };

                                                        let _ = tx.send(row).await;
                                                        saved += 1;
                                                    }
                                                }
                                                saved
                                            },
                                            Err(_) => 0,
                                        }
                                    }
                                },
                                Err(_) => 0,
                            }
                        }
                    },
                    Err(_) => 0,
                };
                bar.inc(1);
                saved
            });

            tasks.push(task);

            // Process in batches to avoid overwhelming the system
            if tasks.len() >= args.workers {
                for task in tasks.drain(..) {
                    let _ = task.await;
                }
            }
        }

        // Wait for remaining tasks
        for task in tasks {
            let _ = task.await;
        }

        bar.finish_with_message("✓ Complete");
    }

    drop(tx);
    println!("\nWaiting for database writes to complete...");
    let total = writer_handle.await??;
    println!(
        "✓ Database complete: Wrote {} total rows (deduped by userId)",
        total
    );

    Ok(())
}
