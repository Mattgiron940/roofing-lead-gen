#!/bin/bash

# Batch deployment script for all 5 DFW Apify actors
echo "🚀 Starting batch deployment of DFW Apify actors..."

ACTORS=("dfw-zillow-actor" "dfw-redfin-actor" "dfw-cad-actor" "dfw-permit-actor" "dfw-storm-actor")

# Create proper Apify configuration for each actor
for actor in "${ACTORS[@]}"; do
    echo "📁 Setting up $actor..."
    
    cd "apify_actors/$actor"
    
    # Create .actor directory and modern config
    mkdir -p .actor
    
    # Create actor.json in new format
    cat > .actor/actor.json << EOF
{
    "actorSpecification": 1,
    "name": "$actor",
    "title": "$actor",
    "description": "DFW lead generation actor for roofing business",
    "version": {
        "versionNumber": "0.0.1",
        "buildTag": "latest"
    },
    "dockerfile": "./Dockerfile",
    "input": "./INPUT_SCHEMA.json",
    "storages": {
        "dataset": {}
    }
}
EOF
    
    # Create .gitignore
    cat > .gitignore << EOF
node_modules/
apify_storage/
.DS_Store
EOF
    
    echo "✅ $actor configured"
    cd ../..
done

echo "📊 Configuration Summary:"
echo "• 5 actors created with Supabase integration"
echo "• Each actor targets DFW area specifically"
echo "• Deduplication and lead scoring included"
echo "• Ready for scheduling and deployment"

echo ""
echo "🔧 Next Steps:"
echo "1. Test locally: cd apify_actors/dfw-zillow-actor && apify run"
echo "2. Deploy to platform: cd apify_actors/dfw-zillow-actor && apify push"
echo "3. Configure scheduling in Apify Console"
echo "4. Monitor performance and adjust concurrency"

echo ""
echo "📋 Recommended Schedule:"
echo "• dfw-zillow-actor: Every 4 hours"
echo "• dfw-redfin-actor: Every 3 hours"
echo "• dfw-cad-actor: Every 6 hours"
echo "• dfw-permit-actor: Every 12 hours"
echo "• dfw-storm-actor: Every 24 hours"

echo ""
echo "✅ Batch deployment preparation completed!"