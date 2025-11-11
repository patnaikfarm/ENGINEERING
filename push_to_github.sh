#!/bin/bash

# GitHub Push Helper Script
# This script will help you push your ENGINEERING repository to GitHub

echo "=========================================="
echo "  ENGINEERING Repository - GitHub Push"
echo "=========================================="
echo ""
echo "This script will push your repository to:"
echo "https://github.com/patnaikfarm/ENGINEERING"
echo ""
echo "You need a GitHub Personal Access Token."
echo ""
echo "To create one:"
echo "1. Go to: https://github.com/settings/tokens"
echo "2. Click 'Generate new token (classic)'"
echo "3. Name it: 'ENGINEERING Repo'"
echo "4. Check: ✅ repo (full control)"
echo "5. Click 'Generate token'"
echo "6. COPY the token"
echo ""
echo "=========================================="
echo ""

# Prompt for token (hidden input)
read -sp "Paste your GitHub token here (input hidden): " GITHUB_TOKEN
echo ""
echo ""

# Validate token is not empty
if [ -z "$GITHUB_TOKEN" ]; then
    echo "❌ Error: No token provided!"
    exit 1
fi

echo "✅ Token received!"
echo ""
echo "🔄 Configuring remote with token..."

# Remove existing remote if it exists
git remote remove origin 2>/dev/null

# Add remote with token
git remote add origin https://${GITHUB_TOKEN}@github.com/patnaikfarm/ENGINEERING.git

echo "✅ Remote configured!"
echo ""
echo "🚀 Pushing to GitHub..."
echo ""

# Push to GitHub
if git push -u origin main; then
    echo ""
    echo "=========================================="
    echo "✅ SUCCESS! Repository pushed to GitHub!"
    echo "=========================================="
    echo ""
    echo "🔗 View your repository at:"
    echo "   https://github.com/patnaikfarm/ENGINEERING"
    echo ""
    echo "🎉 Your ENGINEERING portfolio is now live!"
    echo ""
    echo "Next steps:"
    echo "1. Add repository topics: pyspark, data-engineering, etl-pipeline"
    echo "2. Star your own repo to make it more visible"
    echo "3. Share the link on LinkedIn!"
    echo ""
else
    echo ""
    echo "=========================================="
    echo "❌ Push failed!"
    echo "=========================================="
    echo ""
    echo "Possible issues:"
    echo "1. Token doesn't have 'repo' permissions"
    echo "2. Repository name doesn't match (must be 'ENGINEERING')"
    echo "3. Token has expired"
    echo ""
    echo "Please check and try again."
    exit 1
fi

# Clean up - remove token from remote URL for security
echo "🔒 Cleaning up token from git config for security..."
git remote set-url origin https://github.com/patnaikfarm/ENGINEERING.git

echo "✅ Done! Token removed from local config."
echo ""
