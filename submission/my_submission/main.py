"""
    main.py — Interactive CDC pipeline orchestration.

    Demonstrates the complete flow with user-selectable scenarios:
    1. Create source tables
    2. Load sample data
    3. Capture CDC events (with user-selectable patterns)
    4. Write to lake (immutable append-only)
    5. Apply to warehouse (current-state snapshots)
    6. Run validations
    7. Display results & time-travel recovery demo

    Supported scenarios:
    - Read: Query current state
    - Write: Insert/Update/Delete operations
    - Mixed: Combination of operations
    - Time-Travel: Point-in-time recovery demo
"""

import duckdb
from core import (print_section, show_menu, run_full_pipeline,
                  scenario_read_current_state, scenario_write_operations, 
                  scenario_mixed_operations, scenario_time_travel, scenario_validations)




def main() -> int:
    """Run interactive CDC pipeline."""
    
    print_section("E-COMMERCE CDC LAKEHOUSE PIPELINE - INTERACTIVE MODE")
    
    print("Choose a Duckdb storage location:")
    print("""  1️⃣  In-memory (default, ephemeral) \n  2️⃣  Local file (persistent across runs)""")
    storage_choice = input("Enter choice (1-2): ").strip()  
    if storage_choice == "2":
        path = input(("Enter directory for DuckDB file (default: current directory): ")).strip() or "."  
        db_path = f"{path}\cdc_pipeline.duckdb"
        conn = duckdb.connect(db_path)
        print(f"✓ Connected to local DuckDB file: {db_path}\n")
    else:
        db_path = ":memory:"
        conn = duckdb.connect(db_path)
        print("✓ Created in-memory DuckDB connection\n")

    print("Choose from the following scenarios to explore different aspects of the pipeline:\n")
    print("Always start with option 6 to run the full pipeline with sample data, then explore individual scenarios (1-5) for specific features and validations.\n")

    while True:
        choice = show_menu()
        
        if choice == "0":
            print("\n  👋 Exiting...\n")
            break
        elif choice == "1":
            try:
                scenario_read_current_state(conn)
            except Exception as e:
                print(f"  ❌ Error: {e}")
        elif choice == "2":
            try:
                scenario_write_operations(conn)
            except Exception as e:
                print(f"  ❌ Error: {e}")
        elif choice == "3":
            try:
                scenario_mixed_operations(conn)
            except Exception as e:
                print(f"  ❌ Error: {e}")
        elif choice == "4":
            try:
                scenario_time_travel(conn)
            except Exception as e:
                print(f"  ❌ Error: {e}")
        elif choice == "5":
            try:
                scenario_validations(conn)
            except Exception as e:
                print(f"  ❌ Error: {e}")
        elif choice == "6" or choice == "":
            try:
                run_full_pipeline(conn)
            except Exception as e:
                print(f"  ❌ Error: {e}")
        else:
            print(f"  ❌ Invalid choice: {choice}")
    
    return 0


if __name__ == "__main__":
    exit(main())
