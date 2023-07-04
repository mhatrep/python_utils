from PIL import Image, ImageDraw, ImageFont
import random

# An extended palette of harmonious colors to use for the shapes.
COLOR_PALETTE = [
    (236, 208, 120),
    (83, 119, 122),
    (187, 119, 132),
    (218, 91, 71),
    (192, 41, 66),
    (78, 205, 196),
    (199, 244, 100),
    (255, 107, 107),
    (196, 77, 88),
    (121, 44, 122),
]

SHAPE_TYPES = ['rectangle', 'circle', 'line', 'triangle', 'ellipse']

def draw_random_shapes(draw, width, height, num_shapes):
    for _ in range(num_shapes):
        shape_type = random.choice(SHAPE_TYPES)
        color = random.choice(COLOR_PALETTE)
        x0 = random.randint(0, width)
        y0 = random.randint(0, height)
        x1 = random.randint(0, width)
        y1 = random.randint(0, height)
        xy = [min(x0, x1), min(y0, y1), max(x0, x1), max(y0, y1)]
        if shape_type == 'rectangle':
            draw.rectangle(xy, fill=color)
        elif shape_type == 'circle':
            draw.ellipse(xy, fill=color)
        elif shape_type == 'line':
            draw.line(xy, fill=color, width=120)
        elif shape_type == 'triangle':
            draw.polygon([(xy[0], xy[1]), (xy[2], xy[1]), (xy[0], xy[3])], fill=color)
        elif shape_type == 'ellipse':
            draw.ellipse(xy, fill=color)

def create_book_cover(title, author, width=3000, height=4500):
    # Create a new image with a single random RGB value
    img = Image.new('RGB', (width, height), random.choice(COLOR_PALETTE))

    draw = ImageDraw.Draw(img)

    # Draw random shapes
    draw_random_shapes(draw, width, height, num_shapes=10)

    # Get the default font (might vary per system, set as per requirement)
    title_font = ImageFont.truetype('arial.ttf', 300)  # Adjusted font size
    author_font = ImageFont.truetype('arial.ttf', 160)  # Adjusted font size

    # Calculate the width and height of the title and author to center them
    title_w, title_h = draw.textsize(title, font=title_font)
    author_w, author_h = draw.textsize(author, font=author_font)

    title_position = ((width - title_w) / 2, (height - title_h) / 2)
    author_position = ((width - author_w) / 2, height - author_h * 2)

    # Draw semi-transparent rectangles for the text background
    img_with_alpha = img.convert("RGBA")  # Convert image to RGBA mode to support transparency
    draw = ImageDraw.Draw(img_with_alpha)

    draw.rectangle([title_position[0] - 10, title_position[1] - 10, 
                    title_position[0] + title_w + 10, title_position[1] + title_h + 10], 
                   fill=(0, 0, 0, 128))  # Half-transparent black
    draw.rectangle([author_position[0] - 10, author_position[1] - 10, 
                    author_position[0] + author_w + 10, author_position[1] + author_h + 10], 
                   fill=(0, 0, 0, 128))  # Half-transparent black

    # Draw the text onto the image
    draw.text(title_position, title, (255, 255, 255), font=title_font)
    draw.text(author_position, author, (255, 255, 255), font=author_font)

    # Save the image in PNG format (to retain transparency)
    img_with_alpha.save('book_cover.png', 'PNG')

# Use the function
create_book_cover('Fluent Esperanto \n    in 10 Steps', 'Prashant Mhatre')
