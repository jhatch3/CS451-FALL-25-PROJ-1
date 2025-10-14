# GIT 


# Clone
- git clone <repo_url>
- cd <repo_name>

# Create your own branch
- sgit checkout main
- git pull origin main
- git checkout -b feature/<your-feature-name>

# Make changes 
- git add .
- git commit -m "Implement login page"

# make sure to pull main to keep sync before pushing 
- git fetch origin
- git checkout main
- git pull origin main
- git checkout feature/<your-feature-name>
- git rebase main  # or 'git merge main' if your team prefers merging

# Push branch
- git push origin feature/<your-feature-name>

# Open github and look for pull req
