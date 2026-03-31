use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use anyhow::Result;
use clap::Parser;
use indicatif::ProgressBar;
use reqwest::Client;
use rusqlite::params;

const API_USER_TEMPLATE: &str = "https://api.skycards.oldapes.com/users/pub/";

const TOKEN: &str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIwMTljMmVmMi0xMDEyLTcwNjEtYWUzNS0wNjlmMThhZTQyYjIiLCJqdGkiOiJIaVdiZlVFM0lnMjBONkhqYnVWT2pfeGFZa2VlNDRaSVZHcFpXX2htSGVNIiwiaWF0IjoxNzcwODI1MzM3LCJleHAiOjQ5MjY1ODUzMzd9.-fOLPEtuvKqOuLnk2EZT8f_Lf-ymMIp4_vjnVgqzNEo";

const BATCH_SIZE: usize = 500;
const DB_PATH: &str = "data/DB/highscore.db";

#[derive(Parser)]
#[command(about = "Refresh player stats from the Skycards API")]
struct Args {
    #[arg(long, default_value = "12", help = "Number of concurrent workers")]
    workers: usize,
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
    #[serde(alias = "name", alias = "userName", alias = "displayName")]
    user_name: Option<String>,
}

#[derive(Clone)]
struct PlayerUpdate {
    user_id: String,
    user_name: String,
    xp: i32,
    aircraft_count: i32,
    destinations: i32,
    battle_wins: i32,
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
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap()
}

fn get_player_ids() -> Result<Vec<(String, String)>> {
    let conn = rusqlite::Connection::open(DB_PATH)?;
    let mut stmt = conn.prepare("SELECT userId, userName FROM airport_highscore")?;

    let players = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;

    Ok(players)
}

async fn fetch_user_stats(client: &Client, user_id: &str, user_name: &str) -> PlayerUpdate {
    let url = format!("{}{}", API_USER_TEMPLATE, user_id);
    
    match client.get(&url).send().await {
        Ok(resp) => match resp.json::<UserProfile>().await {
            Ok(profile) => PlayerUpdate {
                user_id: user_id.to_string(),
                user_name: profile.user_name.unwrap_or_else(|| user_name.to_string()),
                xp: profile.user_xp.unwrap_or(0),
                aircraft_count: profile.aircraft_count.unwrap_or(0),
                destinations: profile.destinations.unwrap_or(0),
                battle_wins: profile.battle_wins.unwrap_or(0),
            },
            Err(_) => PlayerUpdate {
                user_id: user_id.to_string(),
                user_name: user_name.to_string(),
                xp: 0,
                aircraft_count: 0,
                destinations: 0,
                battle_wins: 0,
            },
        },
        Err(_) => PlayerUpdate {
            user_id: user_id.to_string(),
            user_name: user_name.to_string(),
            xp: 0,
            aircraft_count: 0,
            destinations: 0,
            battle_wins: 0,
        },
    }
}

fn batch_update_players(updates: Vec<PlayerUpdate>) -> Result<()> {
    let mut conn = rusqlite::Connection::open(DB_PATH)?;
    let tx = conn.transaction()?;

    for update in updates {
        tx.execute(
            "UPDATE airport_highscore SET userName = ?1, userXP = ?2, aircraftCount = ?3, destinations = ?4, battleWins = ?5 WHERE userId = ?6",
            params![
                &update.user_name,
                update.xp,
                update.aircraft_count,
                update.destinations,
                update.battle_wins,
                &update.user_id
            ],
        )?;
    }

    tx.commit()?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();

    let players = get_player_ids()?;
    let total = players.len();
    println!("Starting refresh of {} players with {} workers\n", total, args.workers);

    let pbar = ProgressBar::new(total as u64);
    pbar.set_style(
        indicatif::ProgressStyle::default_bar()
            .template("  {spinner:.green} [{bar:35.cyan/blue}] {pos:>4}/{len:4} players")
            .unwrap()
            .progress_chars("▓▒░")
            .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
    );

    let client = create_client();
    let semaphore = Arc::new(Semaphore::new(args.workers));
    let mut tasks = Vec::new();

    let (tx, mut rx) = tokio::sync::mpsc::channel::<PlayerUpdate>(BATCH_SIZE);
    let mut batch = Vec::new();

    for (user_id, user_name) in players {
        let client = client.clone();
        let semaphore = Arc::clone(&semaphore);
        let tx = tx.clone();
        let pbar = pbar.clone();

        let task = tokio::spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();
            let update = fetch_user_stats(&client, &user_id, &user_name).await;
            let _ = tx.send(update).await;
            pbar.inc(1);
        });

        tasks.push(task);
    }

    drop(tx);

    // Spawn batch processor
    let batch_task = tokio::spawn(async move {
        while let Some(update) = rx.recv().await {
            batch.push(update);
            if batch.len() >= BATCH_SIZE {
                if let Err(e) = batch_update_players(batch.drain(..).collect()) {
                    eprintln!("Failed to batch update: {}", e);
                }
            }
        }

        // Final batch
        if !batch.is_empty() {
            if let Err(e) = batch_update_players(batch) {
                eprintln!("Failed to batch update (final): {}", e);
            }
        }
    });

    // Wait for all tasks to complete
    for task in tasks {
        let _ = task.await;
    }

    batch_task.await?;
    pbar.finish_with_message("✓ Complete");

    println!("\n✓ Refresh complete! Processed {} players", total);

    Ok(())
}
