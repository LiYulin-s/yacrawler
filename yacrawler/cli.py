import typer
import importlib

cli = typer.Typer()

@cli.command()
def run(app_path: str):
    """
    Run a user-defined app, e.g. `myproject.app`
    """
    try:
        # 1. 解析模块路径和变量名
        if ":" in app_path:
            module_name, attr = app_path.split(":")
        else:
            module_name, attr = app_path.rsplit(".", 1)

        # 2. 动态导入模块
        module = importlib.import_module(module_name)

        # 3. 获取 app 对象
        app = getattr(module, attr)

        # 4. 运行 app
        app.run()

    except Exception as e:
        typer.echo(f"❌ Failed to run app from '{app_path}': {e}", err=True)
        raise typer.Exit(1)
