import axios, { AxiosResponse } from "axios";
import Database from "better-sqlite3";
import https from "https";

// 1. Connect the SQL Database
const db = new Database("./data.db", {
    verbose: console.log
});

// 2. Create the tables for this SQL Database if they don't already exist.
// From https://stackoverflow.com/q/61857380
const createPageNumberTable = `
    CREATE TABLE IF NOT EXISTS PageNumber(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        page INTEGER NOT NULL, 
        datetime TEXT NOT NULL
    )
`;
const createTallinnTable = `
    CREATE TABLE IF NOT EXISTS TallinnData(
        id INTEGER PRIMARY KEY,
        andurid_id INTEGER,
        name VARCHAR(100),
        ts TEXT,
        pir INTEGER,
        in_count INTEGER,
        out_count INTEGER,
        humidity REAL,
        temp REAL
    )
`;
console.log(JSON.stringify(createPageNumberTable))
db.prepare(createPageNumberTable).run();
db.prepare(createTallinnTable).run();

// 3. Function should retrieve which page it's currently at.
//      OR start a new entry at 1
interface PageNumberRow {
    id: number,
    page: number,
    datetime: string
}
interface TallinnAPIRow {
    id : number,
    andurid_id : number,
    name : string,
    ts : string,
    pir : number,
    in: number,
    out: number,
    humidity: number | null,
    temp : number | null
}
interface TallinnTableRow {
    id : number,
    andurid_id : number,
    name : string,
    ts : string,
    pir : number,
    in_count: number,
    out_count: number,
    humidity: number | null,
    temp : number | null
}

let statement1 = db.prepare<[], PageNumberRow>(`
    SELECT * FROM PageNumber ORDER BY datetime DESC LIMIT 1`
)
let pageEntry = statement1.get();
let page = pageEntry?.page ?? 1;
if (page !== 1) { page = page + 1 } //Because this is the last page we did.
let results_per_page = 1000;

// 4. Do the querying
// From: https://stackoverflow.com/a/47480429
const delay = (ms : number) => new Promise(res => setTimeout(res, ms));

// While (2869800 pages / 1000 = 2869.800 => 2870 pages.)
const updatePage = () : number => {
    let now = new Date();
    let isoString = now.toISOString();

    db.prepare(`
        INSERT INTO PageNumber(page, datetime)
        VALUES (:page, :isoString)
    `).run({page, isoString});
    return page + 1
}

const callAxios = async (page : number, results_per_page : number) => {
    const res : AxiosResponse = await axios.get(
        process.env.URL ?? 
        `https://avaandmed.tallinn.ee/data/?table=andurid_data&page=${page}&per_page=${results_per_page}`,
        {
            httpsAgent: new https.Agent({
                rejectUnauthorized : false
            })
        }
    )
    let data : Array<TallinnAPIRow> = res.data;
    return data
}

//     while page <= 2870 and keep_going (because it starts at 1)
//     a) Query using the page & number of results
//     b) If it fails, retry after 5 seconds.
//          allow up to three retries before canceling (cancel by keep_going)
//          keep_going = False
//     c) If it succeeds,
//          page += 1
//          create a new entry in the pageEntry DB of the last page queried.
//          save one new entry in the DB per each item in the API response. 

const queryEstonia = async (testing : boolean) => {
    let keep_going = true;
    let attempts = 1;
    while (keep_going === true && page <= 2870) {
        try {
            let results : TallinnAPIRow[] = await callAxios(
                page,
                results_per_page
            )
            await delay(8000);
            attempts = 1;
            if(testing) {
                console.log("Process was marked as testing. returning...")
                //console.log(results)
                await delay(5000);
                return;
            }
            page = updatePage();
            if (results.length < 1) {
                console.log("no results were returned (length < 1)")
                keep_going = false
            } else {
                console.log("Got results, saving to DB")
                const insertMany = db.transaction((rows: TallinnAPIRow[]) => {
                    for (const row of rows) {
                        db.prepare(`
                            INSERT INTO TallinnData (id, andurid_id, name, ts, pir, in_count, out_count, humidity, temp)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        `).run(
                            row.id,
                            row.andurid_id,
                            row.name,
                            row.ts,
                            row.pir,
                            row.in,
                            row.out,
                            row.humidity,
                            row.temp
                        )
                    }
                });
                insertMany(results)
            }
        } catch(error) {
            console.log(`Something went wrong... Attempt ${attempts}`)
            console.log(error)
            await delay(8000);
            attempts = attempts + 1
            if (attempts > 100) {
                console.log("Attempts exceed limit. Stopping...")
                keep_going = false;
            }
        }
    }
}

(async () =>  {
    console.log("Running queryEstonia")
    await queryEstonia(false)
    console.log("Finished process, script exiting (did it work?)")
})();
