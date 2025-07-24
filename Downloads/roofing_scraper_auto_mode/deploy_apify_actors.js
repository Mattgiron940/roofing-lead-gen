#!/usr/bin/env node

/**
 * Enterprise Apify Actor Deployment Script
 * Deploys 5 DFW lead generation actors with Supabase integration
 */

const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

// Actor configurations
const ACTORS = [
    {
        name: 'dfw-zillow-actor',
        description: 'Zillow lead scraper for DFW area',
        schedule: 'every 4 hours',
        table: 'zillow_leads'
    },
    {
        name: 'dfw-redfin-actor', 
        description: 'Redfin lead scraper for DFW area',
        schedule: 'every 3 hours',
        table: 'redfin_leads'
    },
    {
        name: 'dfw-cad-actor',
        description: 'County CAD property data scraper',
        schedule: 'every 6 hours', 
        table: 'cad_leads'
    },
    {
        name: 'dfw-permit-actor',
        description: 'Building permit scraper for DFW municipalities',
        schedule: 'every 12 hours',
        table: 'permit_leads'
    },
    {
        name: 'dfw-storm-actor',
        description: 'NOAA and HailTrace storm data collector',
        schedule: 'every 24 hours',
        table: 'storm_leads'
    }
];

const SUPABASE_CONFIG = {
    url: 'https://rupqnhgtzfynvzgxkgch.supabase.co',
    key: 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJ1cHFuaGd0emZ5bnZ6Z3hrZ2NoIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTMzMDc1NzEsImV4cCI6MjA2ODg4MzU3MX0.kVIh0HhG2BUjqptokZM_ci9G0cFeCPNtv3wUxRxts0c'
};

// Common package.json template
const PACKAGE_TEMPLATE = {
    version: "1.0.0",
    main: "main.js",
    scripts: {
        start: "node main.js"
    },
    dependencies: {
        "apify": "^3.0.0",
        "cheerio": "^1.0.0-rc.12", 
        "crawlee": "^3.0.0",
        "axios": "^1.6.0",
        "@supabase/supabase-js": "^2.0.0"
    },
    author: "DFW Roofing Lead Engine",
    license: "MIT"
};

// Common actor template parts
const ACTOR_HEADER = `import { Actor } from 'apify';
import { CheerioCrawler } from 'crawlee';
import { createClient } from '@supabase/supabase-js';
import axios from 'axios';

// Initialize Supabase client
const supabaseUrl = '${SUPABASE_CONFIG.url}';
const supabaseKey = '${SUPABASE_CONFIG.key}';
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
    const key = \`\${lead.address?.toLowerCase().trim()}_\${lead.zip_code}\`;
    if (processedLeads.has(key)) return true;
    processedLeads.add(key);
    return false;
}

// Utility functions
function extractPrice(priceText) {
    if (!priceText) return null;
    const cleaned = priceText.replace(/[^\\d]/g, '');
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
}`;

console.log('🚀 Starting Apify Actor Deployment...');
console.log(`📊 Deploying ${ACTORS.length} actors for DFW lead generation`);

// Create actor directories and files
for (const actor of ACTORS) {
    console.log(`\\n🔧 Creating ${actor.name}...`);
    
    const actorDir = path.join('apify_actors', actor.name);
    
    // Create directory
    if (!fs.existsSync(actorDir)) {
        fs.mkdirSync(actorDir, { recursive: true });
    }
    
    // Create package.json
    const packageJson = {
        name: actor.name,
        description: actor.description,
        ...PACKAGE_TEMPLATE
    };
    
    fs.writeFileSync(
        path.join(actorDir, 'package.json'),
        JSON.stringify(packageJson, null, 2)
    );
    
    // Create main.js based on actor type
    let mainJs = ACTOR_HEADER;
    
    if (actor.name.includes('zillow')) {
        mainJs += `

// Zillow-specific extraction
function extractZillowData($, url) {
    const properties = [];
    
    $('article[data-test="property-card"], .list-card').each((index, element) => {
        const $card = $(element);
        
        const property = {
            source: 'zillow',
            source_url: url,
            scraped_at: new Date().toISOString(),
            dfw: true,
            
            address: $card.find('[data-test="property-card-addr"]').text().trim(),
            price: extractPrice($card.find('[data-test="property-card-price"]').text()),
            bedrooms: parseInt($card.find('[data-test="property-card-details"]').text().match(/(\\d+)\\s*bds?/i)?.[1]) || null,
            bathrooms: parseFloat($card.find('[data-test="property-card-details"]').text().match(/(\\d+(?:\\.\\d+)?)\\s*ba/i)?.[1]) || null,
            square_feet: parseInt($card.find('[data-test="property-card-details"]').text().match(/([\\d,]+)\\s*sqft/i)?.[1]?.replace(/,/g, '')) || null,
            property_url: $card.find('a').attr('href'),
            lead_score: 5
        };
        
        if (property.address && property.address.length > 10) {
            property.lead_score = calculateLeadScore(property);
            properties.push(property);
        }
    });
    
    return properties;
}

await Actor.main(async () => {
    console.log('🏠 Starting DFW Zillow Actor...');
    
    const urls = [];
    for (const zipCode of DFW_ZIP_CODES.slice(0, 20)) {
        urls.push(\`https://www.zillow.com/homes/\${zipCode}_rb/sold_type/\`);
        urls.push(\`https://www.zillow.com/homes/\${zipCode}_rb/for_sale_type/\`);
    }
    
    const crawler = new CheerioCrawler({
        async requestHandler({ $, request }) {
            console.log(\`Processing: \${request.url}\`);
            
            const properties = extractZillowData($, request.url);
            let insertedCount = 0;
            
            for (const lead of properties) {
                if (!isDuplicateLead(lead)) {
                    try {
                        await supabase.from('${actor.table}').insert(lead);
                        insertedCount++;
                    } catch (error) {
                        console.error('Insert error:', error);
                    }
                }
            }
            
            console.log(\`✅ Inserted \${insertedCount} leads\`);
        },
        maxConcurrency: 3,
        maxRequestsPerCrawl: urls.length
    });
    
    await crawler.addRequests(urls.map(url => ({ url })));
    await crawler.run();
    
    console.log('✅ Zillow Actor completed!');
});`;
    
    } else if (actor.name.includes('redfin')) {
        mainJs += `

// Redfin-specific extraction  
function extractRedfinData($, url) {
    const properties = [];
    
    $('.HomeCard, .home-card').each((index, element) => {
        const $card = $(element);
        
        const property = {
            source: 'redfin',
            source_url: url,
            scraped_at: new Date().toISOString(),
            dfw: true,
            
            address: $card.find('.homeAddress').text().trim(),
            price: extractPrice($card.find('.price').text()),
            bedrooms: parseInt($card.find('.HomeStats').text().match(/(\\d+)\\s*bed/i)?.[1]) || null,
            bathrooms: parseFloat($card.find('.HomeStats').text().match(/(\\d+(?:\\.\\d+)?)\\s*bath/i)?.[1]) || null,
            square_feet: parseInt($card.find('.HomeStats').text().match(/([\\d,]+)\\s*sq/i)?.[1]?.replace(/,/g, '')) || null,
            lead_score: 5
        };
        
        if (property.address && property.address.length > 10) {
            property.lead_score = calculateLeadScore(property);
            properties.push(property);
        }
    });
    
    return properties;
}

await Actor.main(async () => {
    console.log('🏠 Starting DFW Redfin Actor...');
    
    const cities = ['Dallas', 'Fort-Worth', 'Arlington', 'Plano', 'Irving'];
    const urls = [];
    
    for (const city of cities) {
        urls.push(\`https://www.redfin.com/city/\${city.toLowerCase()}/filter/sold-7da\`);
        urls.push(\`https://www.redfin.com/city/\${city.toLowerCase()}/filter/property-type=house\`);
    }
    
    const crawler = new CheerioCrawler({
        async requestHandler({ $, request }) {
            console.log(\`Processing: \${request.url}\`);
            
            const properties = extractRedfinData($, request.url);
            let insertedCount = 0;
            
            for (const lead of properties) {
                if (!isDuplicateLead(lead)) {
                    try {
                        await supabase.from('${actor.table}').insert(lead);
                        insertedCount++;
                    } catch (error) {
                        console.error('Insert error:', error);
                    }
                }
            }
            
            console.log(\`✅ Inserted \${insertedCount} leads\`);
        },
        maxConcurrency: 3,
        maxRequestsPerCrawl: urls.length
    });
    
    await crawler.addRequests(urls.map(url => ({ url })));
    await crawler.run();
    
    console.log('✅ Redfin Actor completed!');
});`;
    
    } else if (actor.name.includes('cad')) {
        mainJs += `

// CAD-specific extraction
function extractCADData($, url) {
    const properties = [];
    
    $('tr.PropertyRow, .property-record').each((index, element) => {
        const $row = $(element);
        
        const property = {
            source: 'cad',
            source_url: url,
            scraped_at: new Date().toISOString(),
            dfw: true,
            
            address: $row.find('.PropertyAddress').text().trim(),
            owner_name: $row.find('.OwnerName').text().trim(),
            assessed_value: extractPrice($row.find('.AssessedValue').text()),
            property_type: 'residential',
            lead_score: 6 // CAD data gets higher score
        };
        
        if (property.address && property.address.length > 10) {
            properties.push(property);
        }
    });
    
    return properties;
}

await Actor.main(async () => {
    console.log('🏛️ Starting DFW CAD Actor...');
    
    const cadOffices = [
        'https://www.dallascad.org/PropertySearch/search.aspx',
        'https://www.tad.org/PropSearch/search.aspx',
        'https://www.collincad.org/Property-Search',
        'https://www.dentoncad.com/property-search'
    ];
    
    const crawler = new CheerioCrawler({
        async requestHandler({ $, request }) {
            console.log(\`Processing: \${request.url}\`);
            
            const properties = extractCADData($, request.url);
            let insertedCount = 0;
            
            for (const lead of properties) {
                if (!isDuplicateLead(lead)) {
                    try {
                        await supabase.from('${actor.table}').insert(lead);
                        insertedCount++;
                    } catch (error) {
                        console.error('Insert error:', error);
                    }
                }
            }
            
            console.log(\`✅ Inserted \${insertedCount} leads\`);
        },
        maxConcurrency: 2,
        maxRequestsPerCrawl: cadOffices.length
    });
    
    await crawler.addRequests(cadOffices.map(url => ({ url })));
    await crawler.run();
    
    console.log('✅ CAD Actor completed!');
});`;
    
    } else if (actor.name.includes('permit')) {
        mainJs += `

// Permit-specific extraction
function extractPermitData($, url) {
    const permits = [];
    
    $('tr.PermitRow, .permit-record').each((index, element) => {
        const $row = $(element);
        
        const permit = {
            source: 'permits',
            source_url: url,
            scraped_at: new Date().toISOString(),
            dfw: true,
            
            permit_number: $row.find('.PermitNumber').text().trim(),
            address: $row.find('.PermitAddress').text().trim(),
            work_description: $row.find('.WorkDescription').text().trim(),
            permit_value: extractPrice($row.find('.PermitValue').text()),
            contractor_name: $row.find('.Contractor').text().trim(),
            lead_score: 7 // Permits are high-quality leads
        };
        
        // Filter for roofing-related permits
        const description = permit.work_description.toLowerCase();
        if (description.includes('roof') || description.includes('storm') || 
            description.includes('hail') || description.includes('wind')) {
            permits.push(permit);
        }
    });
    
    return permits;
}

await Actor.main(async () => {
    console.log('🏗️ Starting DFW Permit Actor...');
    
    const permitSystems = [
        'https://buildinginspection.dallascityhall.com/PermitSearch/',
        'https://fortworthtexas.gov/permits',
        'https://www.arlingtontx.gov/departments/planning_development/building_inspection',
        'https://www.plano.gov/permits',
        'https://www.cityofirving.org/permits-inspections',
        'https://www.garlandtx.gov/departments/building-inspection/permits'
    ];
    
    const crawler = new CheerioCrawler({
        async requestHandler({ $, request }) {
            console.log(\`Processing: \${request.url}\`);
            
            const permits = extractPermitData($, request.url);
            let insertedCount = 0;
            
            for (const lead of permits) {
                if (!isDuplicateLead(lead)) {
                    try {
                        await supabase.from('${actor.table}').insert(lead);
                        insertedCount++;
                    } catch (error) {
                        console.error('Insert error:', error);
                    }
                }
            }
            
            console.log(\`✅ Inserted \${insertedCount} permits\`);
        },
        maxConcurrency: 2,
        maxRequestsPerCrawl: permitSystems.length
    });
    
    await crawler.addRequests(permitSystems.map(url => ({ url })));
    await crawler.run();
    
    console.log('✅ Permit Actor completed!');
});`;
    
    } else if (actor.name.includes('storm')) {
        mainJs += `

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
            await supabase.from('${actor.table}').insert(report);
            insertedCount++;
            console.log(\`✅ Inserted storm report: \${report.event_type} in \${report.location}\`);
        } catch (error) {
            console.error('Insert error:', error);
        }
    }
    
    console.log(\`✅ Storm Actor completed! Inserted \${insertedCount} reports\`);
});`;
    }
    
    fs.writeFileSync(path.join(actorDir, 'main.js'), mainJs);
    
    // Create INPUT_SCHEMA.json
    const inputSchema = {
        title: `${actor.name} Input`,
        type: "object",
        schemaVersion: 1,
        properties: {
            maxConcurrency: {
                title: "Max Concurrency",
                type: "integer",
                description: "Maximum concurrent requests",
                default: 3,
                minimum: 1,
                maximum: 10
            },
            scraperApiKey: {
                title: "ScraperAPI Key",
                type: "string", 
                description: "Your ScraperAPI key for proxy rotation",
                isSecret: true
            }
        },
        required: []
    };
    
    fs.writeFileSync(
        path.join(actorDir, 'INPUT_SCHEMA.json'),
        JSON.stringify(inputSchema, null, 2)
    );
    
    console.log(`✅ ${actor.name} created successfully`);
}

console.log('\\n🚀 All actors created! Next steps:');
console.log('1. Deploy actors to Apify platform:');
ACTORS.forEach(actor => {
    console.log(`   cd apify_actors/${actor.name} && apify push`);
});
console.log('\\n2. Configure scheduling:');
ACTORS.forEach(actor => {
    console.log(`   ${actor.name}: ${actor.schedule}`);
});
console.log('\\n3. Monitor performance and adjust concurrency as needed');
console.log('\\n✅ Deployment script completed!');