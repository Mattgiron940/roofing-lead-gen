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


// Contact extraction utilities
function extractPhone(text) {
    if (!text) return null;
    
    // Common phone number patterns
    const phonePatterns = [
        /(\+1[-.\s]?)?\(?([0-9]{3})\)?[-.\s]?([0-9]{3})[-.\s]?([0-9]{4})/g,
        /(\+1[-.\s]?)?([0-9]{3})[-.\s]?([0-9]{3})[-.\s]?([0-9]{4})/g,
        /\(?([0-9]{3})\)?[-.\s]?([0-9]{3})[-.\s]?([0-9]{4})/g
    ];
    
    for (const pattern of phonePatterns) {
        const match = text.match(pattern);
        if (match) {
            // Clean and format phone number
            const cleaned = match[0].replace(/[^0-9+]/g, '');
            if (cleaned.length >= 10) {
                return formatPhoneNumber(cleaned);
            }
        }
    }
    
    return null;
}

function extractEmail(text) {
    if (!text) return null;
    
    const emailPattern = /[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}/g;
    const match = text.match(emailPattern);
    
    if (match && match[0]) {
        const email = match[0].toLowerCase();
        // Filter out common non-contact emails
        const blockedDomains = ['noreply', 'donotreply', 'no-reply', 'system', 'admin'];
        if (!blockedDomains.some(domain => email.includes(domain))) {
            return email;
        }
    }
    
    return null;
}

function formatPhoneNumber(phone) {
    // Remove all non-digits except +
    const cleaned = phone.replace(/[^0-9+]/g, '');
    
    // Format as +1-XXX-XXX-XXXX if US number
    if (cleaned.length === 10) {
        return `+1-${cleaned.slice(0,3)}-${cleaned.slice(3,6)}-${cleaned.slice(6)}`;
    } else if (cleaned.length === 11 && cleaned.startsWith('1')) {
        return `+${cleaned.slice(0,1)}-${cleaned.slice(1,4)}-${cleaned.slice(4,7)}-${cleaned.slice(7)}`;
    }
    
    return cleaned;
}

function extractContactInfo($element) {
    const text = $element.text();
    
    return {
        phone: extractPhone(text),
        email: extractEmail(text)
    };
}

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
            lead_score: 7,
                
                // Extract contact information
                phone: extractPhone($row.text()),
                email: extractEmail($row.text()) // Permits are high-quality leads
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
            console.log(`Processing: ${request.url}`);
            
            const permits = extractPermitData($, request.url);
            let insertedCount = 0;
            
            for (const lead of permits) {
                if (!isDuplicateLead(lead)) {
                    try {
                        await supabase.from('permit_leads').insert(lead);
                        insertedCount++;
                    } catch (error) {
                        console.error('Insert error:', error);
                    }
                }
            }
            
            console.log(`✅ Inserted ${insertedCount} permits`);
        },
        maxConcurrency: 2,
        maxRequestsPerCrawl: permitSystems.length
    });
    
    await crawler.addRequests(permitSystems.map(url => ({ url })));
    await crawler.run();
    
    console.log('✅ Permit Actor completed!');
});