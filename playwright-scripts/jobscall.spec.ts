import { test, expect } from '@playwright/test';
import * as fs from 'fs';
import * as path from 'path';

// Main interface for data stored in Node.js
interface JobData {
    title: string;
    company: string;
    postDate: string;
    description: string;
}

test('Enhanced JobsCall.me Multi-Job Crawler', async ({ page }) => {
  test.setTimeout(180000); // 3 minutes timeout
  
  console.log('Starting JobsCall.me crawler...');
  
  await page.goto('https://www.jobscall.me/', { waitUntil: 'networkidle' });
  
  const jobLinks = await page.evaluate(() => {
    const links = Array.from(document.querySelectorAll('a[href^="/job/"]'))
      .filter(link => {
        const href = link.getAttribute('href') || '';
        return !href.includes('?tag=') && !href.includes('?page=');
      })
      .map(link => ({
        url: link.getAttribute('href') || '',
        title: link.textContent?.trim() || ''
      }));
    
    return links.filter((link, index, self) => 
      index === self.findIndex(t => t.url === link.url)
    );
  });
  
  console.log(`Found ${jobLinks.length} unique page links to process.`);
  
  const jobsData: JobData[] = [];
  const maxPages = 10;
  
  for (let i = 0; i < Math.min(jobLinks.length, maxPages); i++) {
    const jobLink = jobLinks[i];
    const fullUrl = `https://www.jobscall.me${jobLink.url}`;
    
    console.log(`\nProcessing page ${jobLink.url} ${i + 1}/${maxPages}: ${jobLink.title}`);
    
    try {
      await page.goto(fullUrl, { waitUntil: 'domcontentloaded' });
      
      // Extract job data with company information
      const jobsFromPage = await page.evaluate(() => {
        interface JobData {
            title: string;
            company: string;
            postDate: string;
            description: string;
        }

        const jobs: JobData[] = [];
        const contentNode = document.querySelector('.sqs-html-content');

        if (!contentNode) {
            return [];
        }
        
        // Extract company from various possible locations outside .sqs-html-content
        let company = 'Unknown Company';
        
        // Try different selectors for company name
        const companySelectors = [
          '.entry-title', 
          '.blog-item-title',
          'h1.entry-title',
          '.entry-header h1',
          '.blog-title'
        ];
        
        for (const selector of companySelectors) {
          const element = document.querySelector(selector);
          if (element && element.textContent?.trim()) {
            company = element.textContent.trim();
            break;
          }
        }
        
        // If still not found, try to extract from page title or meta
        if (company === 'Unknown Company') {
          const pageTitle = document.title;
          if (pageTitle && !pageTitle.includes('jobscall')) {
            company = pageTitle.split(' - ')[0] || pageTitle;
          }
        }
        
        // Extract post date from time.published element
        let postDate = 'Unknown Date';
        const timeElement = document.querySelector('time.published');
        if (timeElement) {
          // Try to get from datetime attribute first
          postDate = timeElement.getAttribute('datetime') || timeElement.textContent?.trim() || 'Unknown Date';
        } else {
          // Fallback: look for any time element or date pattern
          const anyTimeElement = document.querySelector('time');
          if (anyTimeElement) {
            postDate = anyTimeElement.getAttribute('datetime') || anyTimeElement.textContent?.trim() || 'Unknown Date';
          }
        }
        
        type JobInProgress = {
            title: string;
            company: string;
            postDate: string;
            descriptionParts: string[];
        };

        let currentJob: JobInProgress | null = null;
        
        for (const node of Array.from(contentNode.childNodes)) {
            if (node.nodeName === 'H1') {
                if (currentJob) {
                    jobs.push({
                        title: currentJob.title,
                        company: currentJob.company,
                        postDate: currentJob.postDate,
                        description: currentJob.descriptionParts.join('\n\n').trim()
                    });
                }
                currentJob = {
                    title: node.textContent?.trim() || 'Untitled',
                    company: company,
                    postDate: postDate,
                    descriptionParts: []
                };
            } else if (currentJob && node.textContent?.trim()) {
                currentJob.descriptionParts.push(node.textContent.trim());
            }
        }

        if (currentJob) {
            jobs.push({
                title: currentJob.title,
                company: currentJob.company,
                postDate: currentJob.postDate,
                description: currentJob.descriptionParts.join('\n\n').trim()
            });
        }

        return jobs;
      });
      
      if (jobsFromPage.length > 0) {
        jobsData.push(...jobsFromPage);
        console.log(`  > Found ${jobsFromPage.length} job(s) on this page.`);
      } else {
        console.log(`  > No jobs found on this page.`);
      }
      
      await page.waitForTimeout(1500);
      
    } catch (error) {
      console.error(`Error processing ${jobLink.title}:`, error);
    }
  }
  
  // Save data
  const now = new Date();
  const timestamp = `${now.getFullYear()}${String(now.getMonth() + 1).padStart(2, '0')}${String(now.getDate()).padStart(2, '0')}${String(now.getSeconds()).padStart(2, '0')}`;
  const outputDir = path.join(process.cwd(), 'job-data', timestamp);
  
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
  }
  
  const jsonPath = path.join(outputDir, `jobscall-${timestamp}.json`);
  fs.writeFileSync(jsonPath, JSON.stringify(jobsData, null, 2), 'utf8');
  
  const csvFiles: string[] = [];
  jobsData.forEach((job, index) => {
    // Create safe company name for filename
    const safeCompany = job.company
      .replace(/[<>:"/\\|?*]/g, '') // Remove Windows invalid filename characters
      .replace(/\s+/g, '_') // Replace spaces with underscores
      .substring(0, 20); // Limit length to 20 characters
    
    // Create safe job title for filename
    const safeTitle = job.title
      .replace(/[<>:"/\\|?*]/g, '') // Remove Windows invalid filename characters
      .replace(/\s+/g, '_') // Replace spaces with underscores
      .substring(0, 30); // Limit length to 30 characters
    
    const jobFileName = `jobscallme-${String(index + 1).padStart(3, '0')}-${safeCompany}-${safeTitle}-${timestamp}.csv`;
    const csvPath = path.join(outputDir, jobFileName);
    
    // Create CSV content with proper UTF-8 encoding
    const csvContent = [
      'Field,Value',
      `"Title","${job.title.replace(/"/g, '""')}"`,
      `"Company","${job.company.replace(/"/g, '""')}"`,
      `"Post Date","${job.postDate.replace(/"/g, '""')}"`,
      `"Description","${job.description.replace(/"/g, '""').replace(/\n/g, ' | ')}"`
    ].join('\n');
    
    // Write with explicit UTF-8 BOM for better Chinese character support
    const utf8BOM = '\uFEFF';
    fs.writeFileSync(csvPath, utf8BOM + csvContent, 'utf8');
    csvFiles.push(jobFileName);
  });
  
  console.log(`\n✅ Crawling completed!`);
  console.log(`📊 Total jobs crawled: ${jobsData.length}`);
  console.log(`📁 Files saved in '${outputDir}':`);
  console.log(`   - JSON (master): ${path.basename(jsonPath)}`);
  console.log(`   - Individual CSV files: ${csvFiles.length} files`);
  
  expect(jobsData.length).toBeGreaterThan(0);
});
