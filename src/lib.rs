#[allow(warnings)]
mod bindings;
use std::process::Command;
use bindings::{
    exports::supabase::wrappers::routines::Guest,
    supabase::wrappers::{
        http,
        types::{Cell, Context, FdwError, OptionsType, FdwResult, Row},
    },
};

#[derive(Debug, Default)]
struct HelloWorldFdw {
    row_cnt: i32,
    command: String,
}

static mut INSTANCE: *mut HelloWorldFdw = std::ptr::null_mut::<HelloWorldFdw>();

impl HelloWorldFdw {
    fn init() {
        let instance = Self::default();
        unsafe {
            INSTANCE = Box::leak(Box::new(instance));
        }
    }

    fn this_mut() -> &'static mut Self {
        unsafe { &mut (*INSTANCE) }
    }

}

impl Guest for HelloWorldFdw {
    fn host_version_requirement() -> String {
        // semver ref: https://docs.rs/semver/latest/semver/enum.Op.html
        "^0.1.0".to_string()
    }

    fn init(ctx: &Context) -> FdwResult {
        Self::init();
        let this = Self::this_mut();
        
        let opts = ctx.get_options(OptionsType::Server);
        this.command = opts.require_or("command", "whoami");

        Ok(())
    }

    fn begin_scan(_ctx: &Context) -> FdwResult {
        let this = Self::this_mut();

        // reset row counter
        this.row_cnt = 0;

        Ok(())
    }


    fn iter_scan(ctx: &Context, row: &Row) -> Result<Option<u32>, FdwError> {
        let this = Self::this_mut();
        
        let command = this.command.clone();

        // Execute a shell command
        let output = Command::new("sh")
            .arg("-c")
            .arg(command)
            .output()
            .expect("Failed to execute command");
    
        // Check if the command was successful
        if !output.status.success() {
            return Err(format!(
                "Command failed with status: {}",
                output.status
            ));
        }
    
        // Convert the command output to a String
        let command_output = String::from_utf8_lossy(&output.stdout);
    
        if this.row_cnt >= 1 {
            // return 'None' to stop data scan
            return Ok(None);
        }
    
        for tgt_col in &ctx.get_columns() {
            match tgt_col.name().as_str() {
                "id" => {
                    row.push(Some(&Cell::I64(42)));
                }
                "col" => {
                    row.push(Some(&Cell::String(command_output.to_string())));
                }
                _ => unreachable!(),
            }
        }
    
        this.row_cnt += 1;
    
        // return Some(_) to Postgres and continue data scan
        Ok(Some(0))
    }
    


    fn re_scan(_ctx: &Context) -> FdwResult {
        // reset row counter
        let this = Self::this_mut();
        this.row_cnt = 0;
        Ok(())
    }

    fn end_scan(_ctx: &Context) -> FdwResult {
        Ok(())
    }

    fn begin_modify(_ctx: &Context) -> FdwResult {
        unimplemented!("update on foreign table is not supported");
    }

    fn insert(_ctx: &Context, _row: &Row) -> FdwResult {
        unimplemented!("update on foreign table is not supported");
    }

    fn update(_ctx: &Context, _rowid: Cell, _row: &Row) -> FdwResult {
        unimplemented!("update on foreign table is not supported");
    }

    fn delete(_ctx: &Context, _rowid: Cell) -> FdwResult {
        unimplemented!("update on foreign table is not supported");
    }

    fn end_modify(_ctx: &Context) -> FdwResult {
        unimplemented!("update on foreign table is not supported");
    }
}

bindings::export!(HelloWorldFdw with_types_in bindings);
