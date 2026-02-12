import asyncio
import logging
from datetime import datetime, time, timedelta

# Avoid circular imports by importing inside the function
def setup_daily_scheduler(app):
    """
    Sets up a simple background task to run the pipeline daily.
    """
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("Scheduler")

    async def scheduler_loop():
        from run_pipeline import run_full_analytical_pipeline
        
        # Target time: daily at 03:00 AM
        TARGET_HOUR = 3
        
        logger.info(f"Scheduler started. Targeting {TARGET_HOUR}:00 AM daily.")
        
        while True:
            now = datetime.now()
            target_today = datetime.combine(now.date(), time(TARGET_HOUR, 0))
            
            if now >= target_today:
                # If we've passed today's 3 AM, target tomorrow's 3 AM
                target_next = target_today + timedelta(days=1)
            else:
                target_next = target_today
                
            sleep_seconds = (target_next - now).total_seconds()
            logger.info(f"Next pipeline run scheduled for: {target_next} (In {sleep_seconds/3600:.2f} hours)")
            
            await asyncio.sleep(sleep_seconds)
            
            logger.info("🚀 Triggering scheduled daily pipeline run...")
            try:
                await run_full_analytical_pipeline()
                logger.info("✅ Scheduled pipeline run complete.")
            except Exception as e:
                logger.error(f"❌ Scheduled pipeline failed: {e}")
                
    # Start the loop in the background
    @app.on_event("startup")
    async def start_scheduler():
        asyncio.create_task(scheduler_loop())
