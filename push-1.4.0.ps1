# push-1.4.0.ps1 — commit + push ทั้งสอง repo
#
# ทั้งสองโฟลเดอร์เป็น git worktree ที่ .git ชี้ไปโฟลเดอร์แม่ สคริปต์นี้จึงรัน git
# จากในโฟลเดอร์ worktree ตรง ๆ (git จะตามไปหา .git เองได้)
#
# วิธีใช้ — เปิด PowerShell แล้ว:
#     cd 'C:\Users\somka\Desktop\งาน\scanner-api-backend-uat'
#     .\push-1.4.0.ps1
#
# ถ้าอยากดูก่อนว่าจะ push อะไร ใช้:  .\push-1.4.0.ps1 -DryRun

param([switch]$DryRun)

$ErrorActionPreference = 'Stop'

$backend  = 'C:\Users\somka\Desktop\งาน\scanner-api-backend-uat'
$frontend = 'C:\Users\somka\Desktop\งาน\pro-scanner-uat'

$backendMsg = @'
1.4.0-free: ตัด BigQuery ออกหมด + เร่งความเร็วบน Render Free

ตัด BigQuery
- ลบ fetch_wave_data_from_bq, get_valid_lpns_for_wave, get_durable_close_summaries,
  load_pack_case_map, calculate_direct_total_qty, load_numeric_branch_master,
  recover_recent_report_syncs, run_close_job_queries_in_background,
  write_delivery_report_summaries, background_refresh_wave, /api/debug-carton
- ลบ flag UAT_SHEETS_ONLY, QC_FEATURE_ENABLED, LEGACY_DELIVERY_REPORT_SYNC_ENABLED
- main.py 5244 -> 3850 บรรทัด

เขียนใหม่บน Sheets (เดิม 500 แน่นอน)
- close-job เขียนแท็บ Branch Close Status
- correct-lpn เขียน audit + ค่าใหม่ลง Scan Transactions กันซ้ำด้วย correction_id
- corrections อ่านประวัติจาก Scan Transactions
- ทุก endpoint ที่ hold ตอบ 423 ไม่ใช่ 500

ความเร็ว
- stale-while-revalidate บน Member Data: cache หมดอายุแล้วคืนทันที (0.4ms) รีเฟรชเบื้องหลัง
- gzip ทุกการอ่าน gviz ผ่าน fetch_gviz_text()
- index ตาม Wave แทนการสแกน 39k แถวต่อ Wave
- memoise รายการต่อ Wave (234us -> 4us)
- snapshot gzip บน /tmp กัน worker restart โหลดใหม่
- เลิก deepcopy บนแถว string ล้วน
- compile regex + cache ชื่อสาขา
- write-back ยัดเข้า cache ไม่ต้องโหลด 39k แถวใหม่
- Booking fan-out เป็น sequential (งาน CPU ล้วนแล้ว)
- health check ไม่แตะเครือข่าย ใช้ ?deep=1 ตรวจ credentials
- keep-alive cron ping ทุก 10 นาที 06:00-20:00 จ.-ส. (~390/750 ชม.)
'@

$frontendMsg = @'
1.4.0-free: เร่งการโหลดหน้าเว็บ + รับมือ 423 จาก backend

- preconnect + dns-prefetch ไป Render และ Apps Script
- ปลุก Render ทันทีใน <head> (window.__earlyWake) ให้ ensureBackendReady ใช้ผลต่อ
- เพิ่ม 423 เข้า dead-letter list ของ processQueue (เดิม retry วนไม่จบ)
- ลด retry ladder ของ Booking 5 รอบ -> 3 รอบ เว้นห่าง 2/4/6 วิ
- sync APP_VERSION = 1.4.0-free ให้ตรง backend
'@

function Push-Repo {
    param([string]$Path, [string]$Message, [string]$Label)

    Write-Host ''
    Write-Host "=== $Label ===" -ForegroundColor Cyan
    Push-Location $Path
    try {
        # โฟลเดอร์เหล่านี้เป็น git worktree ที่ชื่อ branch ในเครื่องไม่ตรงกับ branch
        # ปลายทาง (เช่น codex/optimize-uat-standard -> uat/main) ทำให้ `git push`
        # เปล่า ๆ ไม่ทำงาน จึงต้องอ่าน upstream แล้วระบุปลายทางให้ชัด
        $branch = (git rev-parse --abbrev-ref HEAD).Trim()
        $upstream = (git rev-parse --abbrev-ref --symbolic-full-name '@{upstream}' 2>$null)
        if (-not $upstream) {
            Write-Host "branch: $branch (ไม่มี upstream — ข้าม ต้อง push เองครั้งแรก)" -ForegroundColor Yellow
            return
        }
        $upstream = $upstream.Trim()
        $remote, $remoteBranch = $upstream -split '/', 2
        Write-Host "branch: $branch  ->  $remote/$remoteBranch"

        git status --short
        if (git status --porcelain) {
            if ($DryRun) {
                Write-Host '--- DryRun: จะ commit ด้วยข้อความนี้ ---' -ForegroundColor Yellow
                Write-Host $Message
            }
            else {
                git add -A
                git commit -m $Message
            }
        }
        else {
            Write-Host 'ไม่มีไฟล์ที่เปลี่ยน' -ForegroundColor Yellow
        }

        git fetch $remote --quiet
        $pending = git log --oneline "$remote/$remoteBranch..HEAD"
        if (-not $pending) {
            Write-Host "ไม่มี commit ค้างส่ง $remote/$remoteBranch ตรงกันแล้ว" -ForegroundColor Yellow
            return
        }
        Write-Host 'commit ที่จะ push:' -ForegroundColor Cyan
        $pending | ForEach-Object { Write-Host "  $_" }

        if ($DryRun) {
            Write-Host "--- DryRun: จะรัน  git push $remote HEAD:$remoteBranch ---" -ForegroundColor Yellow
            return
        }
        git push $remote "HEAD:$remoteBranch"
        Write-Host "$Label pushed -> $remote/$remoteBranch" -ForegroundColor Green
    }
    finally { Pop-Location }
}

Push-Repo -Path $backend  -Message $backendMsg  -Label 'Backend (Backed-API-Scan-UAT)'
Push-Repo -Path $frontend -Message $frontendMsg -Label 'Frontend (Pro-Scan-Uat)'

Write-Host ''
Write-Host 'หลัง Render deploy เสร็จ ให้เช็ก 2 อย่าง:' -ForegroundColor Cyan
Write-Host '  1) https://backed-api-scan-uat.onrender.com/api/health?deep=1'
Write-Host '     ต้องได้ google_credentials.ok = true และ member_data.rows > 0'
Write-Host '  2) ตั้ง SCAN_FEATURE_ENABLED = false ใน Render env ให้ตรงกับ index.html'
Write-Host ''
Write-Host 'keep-alive cron จะเริ่มทำงานเองหลัง push แล้วสั่งรันครั้งแรกได้ที่'
Write-Host '  GitHub > Actions > Keep Render awake > Run workflow'
