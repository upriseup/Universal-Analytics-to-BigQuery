import tkinter as tk
from tkinter import messagebox

def on_button_click():
    messagebox.showinfo("Message", "Button clicked!")

# Create the main window
root = tk.Tk()
root.title("UI with Controls")

# Create a label
label = tk.Label(root, text="Welcome to my UI!")
label.pack()

# Create a button
button = tk.Button(root, text="Click Me!", command=on_button_click)
button.pack()

# Create an entry field
entry = tk.Entry(root)
entry.pack()

# Create a checkbox
checkbox_var = tk.BooleanVar()
checkbox = tk.Checkbutton(root, text="Check me", variable=checkbox_var)
checkbox.pack()

# Create a radio button
radio_var = tk.StringVar()
radio_var.set("Option 1")
radio1 = tk.Radiobutton(root, text="Option 1", variable=radio_var, value="Option 1")
radio1.pack()
radio2 = tk.Radiobutton(root, text="Option 2", variable=radio_var, value="Option 2")
radio2.pack()

# Create a dropdown menu
options = ["Option 1", "Option 2", "Option 3"]
dropdown_var = tk.StringVar()
dropdown_var.set(options[0])
dropdown = tk.OptionMenu(root, dropdown_var, *options)
dropdown.pack()

# Run the main event loop
root.mainloop()