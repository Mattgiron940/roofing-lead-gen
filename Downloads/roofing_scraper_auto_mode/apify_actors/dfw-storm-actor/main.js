import { Actor } from 'apify';
import { CheerioCrawler } from 'crawlee';
import { createClient } from '@supabase/supabase-js';
import axios from 'axios';

// Initialize Supabase client
const supabaseUrl = 'https://rupqnhgtzfynvzgxkgch.supabase.co';
const supabaseKey = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJ1cHFuaGd0emZ5bnZ6Z3hrZ2NoIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTMzMDc1NzEsImV4cCI6MjA2ODg4MzU3MX0.kVIh0HhG2BUjqptokZM_ci9G0cFeCPNtv3wUxRxts0c';
const supabase = createClient(supabaseUrl, supabaseKey);

// DFW ZIP codes for targeted scraping
const DFW_ZIP_CODES = [
    '75001', '75002', '75006', '75007', '75010', '75013', '75014', '75015', '75016', '75017',
    '75019', '75020', '75021', '75022', '75023', '75024', '75025', '75026', '75027', '75028',
    '75030', '75032', '75034', '75035', '75038', '75039', '75040', '75041', '75042', '75043',
    '75044', '75048', '75050', '75051', '75052', '75054', '75056', '75057', '75060', '75061',
    '75062', '75063', '75067', '75069', '75070', '75071', '75074', '75075', '75077', '75078',
    '75080', '75081', '75082', '75083', '75085', '75086', '75087', '75088', '75089', '75093',
    '75094', '75098', '75099'
];

// Lead deduplication
const processedLeads = new Set();

function isDuplicateLead(lead) {
    const key = `${lead.address?.toLowerCase().trim()}_${lead.zip_code}`;
    if (processedLeads.has(key)) return true;
    processedLeads.add(key);
    return false;
}

// Utility functions
function extractPrice(priceText) {
    if (!priceText) return null;
    const cleaned = priceText.replace(/[^\d]/g, '');
    return cleaned ? parseInt(cleaned) : null;
}

function calculateLeadScore(property) {
    let score = 5;
    if (property.price) {
        if (property.price > 500000) score += 3;
        else if (property.price > 350000) score += 2;
        else if (property.price > 250000) score += 1;
    }
    return Math.min(Math.max(score, 1), 10);
}

// Storm data extraction
async function fetchStormData() {
    const stormReports = [];
    
    try {
        // NOAA Storm Events API for DFW area
        const noaaUrl = 'https://www.ncdc.noaa.gov/stormevents/choosedates.jsp?statefips=48'; // Texas
        
        // Mock storm data for now - replace with actual API calls
        const mockStormData = [
            {
                event_type: 'Hail',
                location: 'Dallas County, TX',
                magnitude: '1.5 inches',
                date: new Date().toISOString(),
                damage_estimate: 50000
            },
            {
                event_type: 'Wind',
                location: 'Tarrant County, TX', 
                magnitude: '65 mph',
                date: new Date().toISOString(),
                damage_estimate: 25000
            }
        ];
        
        for (const storm of mockStormData) {
            stormReports.push({
                source: 'noaa_storm_events',
                scraped_at: new Date().toISOString(),
                dfw: true,
                
                event_type: storm.event_type,
                location: storm.location,
                magnitude: storm.magnitude,
                event_date: storm.date,
                damage_estimate: storm.damage_estimate,
                lead_score: 9 // Storm events are highest priority
            });
        }
        
    } catch (error) {
        console.error('Error fetching storm data:', error);
    }
    
    return stormReports;
}

await Actor.main(async () => {
    console.log('⛈️ Starting DFW Storm Actor...');
    
    const stormReports = await fetchStormData();
    let insertedCount = 0;
    
    for (const report of stormReports) {
        try {
            await supabase.from('storm_leads').insert(report);
            insertedCount++;
            console.log(`✅ Inserted storm report: ${report.event_type} in ${report.location}`);
        } catch (error) {
            console.error('Insert error:', error);
        }
    }
    
    console.log(`✅ Storm Actor completed! Inserted ${insertedCount} reports`);
});